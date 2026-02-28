// ParallelItemTick.cpp
#include "ParallelItemTick.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <vector>

#include <ll/api/Config.h>
#include <ll/api/coro/CoroTask.h>
#include <ll/api/io/Logger.h>
#include <ll/api/io/LoggerRegistry.h>
#include <ll/api/memory/Hook.h>
#include <ll/api/mod/NativeMod.h>
#include <ll/api/mod/RegisterHelper.h>
#include <ll/api/thread/ServerThreadExecutor.h>

#include <mc/world/actor/Actor.h>
#include <mc/world/actor/item/ItemActor.h>
#include <mc/world/level/BlockSource.h>
#include <mc/world/level/Level.h>
#include <mc/world/level/dimension/Dimension.h>
#include <mc/world/phys/AABB.h>

namespace parallel_item_tick {

// ============================================================
// 全局变量
// ============================================================
Config                          gConfig;
std::shared_ptr<ll::io::Logger> gLogger;
bool                            gStatsRunning = false;
std::unique_ptr<TickWorkerPool> gWorkerPool;

bool gSkipProcessedItems = false;

std::atomic<uint64_t> gTotalTicks{0};
std::atomic<uint64_t> gTotalProcessed{0};
std::atomic<uint64_t> gTotalMerged{0};
std::atomic<uint64_t> gTotalTimeUs{0};
std::atomic<uint64_t> gMaxTimeUs{0};

// ============================================================
// 工具函数
// ============================================================
ll::io::Logger& getLogger() {
    if (!gLogger) {
        gLogger = ll::io::LoggerRegistry::getInstance().getOrCreate("ParallelItemTick");
    }
    return *gLogger;
}

bool loadConfig() {
    auto path = ParallelItemTickMod::getInstance().getSelf().getConfigDir() / "config.json";
    if (!std::filesystem::exists(path)) {
        getLogger().info("Config not found, creating default config.json");
        return saveConfig();
    }
    if (!ll::config::loadConfig(gConfig, path)) {
        getLogger().warn("Failed to load config, using defaults");
    }
    if (gConfig.minParallelCnt < 1) gConfig.minParallelCnt = 16;
    if (gConfig.workerThreads < 0)  gConfig.workerThreads  = 0;
    if (gConfig.workerThreads == 0) {
        int cores             = static_cast<int>(std::thread::hardware_concurrency());
        gConfig.workerThreads = std::max(1, std::min(cores - 1, 7));
    }
    return true;
}

bool saveConfig() {
    auto path = ParallelItemTickMod::getInstance().getSelf().getConfigDir() / "config.json";
    return ll::config::saveConfig(gConfig, path);
}

void startStatsTask() {
    if (gStatsRunning || !gConfig.stats) return;
    gStatsRunning = true;

    ll::coro::keepThis([]() -> ll::coro::CoroTask<> {
        while (gStatsRunning) {
            co_await std::chrono::seconds(5);
            ll::thread::ServerThreadExecutor::getDefault().execute([] {
                if (!gConfig.stats) return;

                auto totalTicks   = gTotalTicks.load(std::memory_order_relaxed);
                auto totalProcess = gTotalProcessed.load(std::memory_order_relaxed);
                auto totalMerged  = gTotalMerged.load(std::memory_order_relaxed);
                auto totalTime    = gTotalTimeUs.load(std::memory_order_relaxed);
                auto maxTime      = gMaxTimeUs.load(std::memory_order_relaxed);

                getLogger().info(
                    "Stats (5s): frames={}, items/frame={:.1f}, merges/frame={:.1f}, "
                    "avgTime={:.2f}ms, maxTime={:.2f}ms",
                    totalTicks,
                    totalTicks ? static_cast<double>(totalProcess) / totalTicks : 0.0,
                    totalTicks ? static_cast<double>(totalMerged) / totalTicks : 0.0,
                    totalTicks ? static_cast<double>(totalTime) / totalTicks / 1000.0 : 0.0,
                    static_cast<double>(maxTime) / 1000.0
                );

                gTotalTicks.store(0, std::memory_order_relaxed);
                gTotalProcessed.store(0, std::memory_order_relaxed);
                gTotalMerged.store(0, std::memory_order_relaxed);
                gTotalTimeUs.store(0, std::memory_order_relaxed);
                gMaxTimeUs.store(0, std::memory_order_relaxed);
            });
        }
        gStatsRunning = false;
    }).launch(ll::thread::ServerThreadExecutor::getDefault());
}

void stopStatsTask() { gStatsRunning = false; }

// ============================================================
// TickWorkerPool
// ============================================================
TickWorkerPool::TickWorkerPool(int numWorkers) : mNumWorkers(numWorkers) {
    for (int i = 0; i < numWorkers; i++) {
        mWorkers.emplace_back([this] { workerMain(); });
    }
}

TickWorkerPool::~TickWorkerPool() {
    {
        std::lock_guard lock(mMutex);
        mShutdown = true;
    }
    mWorkCv.notify_all();
    for (auto& w : mWorkers) {
        if (w.joinable()) w.join();
    }
}

void TickWorkerPool::parallelFor(size_t count, std::function<void(size_t)> const& func) {
    if (count == 0) return;

    if (count < static_cast<size_t>(gConfig.minParallelCnt) || mNumWorkers == 0) {
        for (size_t i = 0; i < count; i++) func(i);
        return;
    }

    {
        std::lock_guard lock(mMutex);
        mWorkFunc      = &func;
        mWorkCount     = count;
        mNextIndex.store(0, std::memory_order_relaxed);
        mActiveWorkers = mNumWorkers;
        mGeneration++;
    }
    mWorkCv.notify_all();

    while (true) {
        size_t idx = mNextIndex.fetch_add(1, std::memory_order_acq_rel);
        if (idx >= count) break;
        func(idx);
    }

    {
        std::unique_lock lock(mMutex);
        mDoneCv.wait(lock, [this] { return mActiveWorkers == 0; });
    }

    mWorkFunc = nullptr;
}

void TickWorkerPool::workerMain() {
    uint64_t lastGen = 0;
    while (true) {
        {
            std::unique_lock lock(mMutex);
            mWorkCv.wait(lock, [this, lastGen] {
                return mShutdown || mGeneration > lastGen;
            });
            if (mShutdown) return;
            lastGen = mGeneration;
        }

        while (true) {
            size_t idx = mNextIndex.fetch_add(1, std::memory_order_acq_rel);
            if (idx >= mWorkCount) break;
            try {
                (*mWorkFunc)(idx);
            } catch (...) {}
        }

        {
            std::lock_guard lock(mMutex);
            mActiveWorkers--;
        }
        mDoneCv.notify_one();
    }
}

// ============================================================
// 纯计算的物理更新 — 只读写传入的 PhysicsResult
// 不调用任何 BDS API，完全线程安全
// ============================================================
static constexpr float GRAVITY       = -0.04f;
static constexpr float DRAG_AIR      = 0.98f;
static constexpr float DRAG_GROUND   = 0.6f;   // 地面额外摩擦（与方块无关的近似值）
static constexpr int   MAX_AGE       = 6000;
static constexpr int   MAX_LIFETIME  = 72000;   // 物品最大生命周期

struct ItemSnapshot {
    // 从主线程读取的快照（只读）
    float posX, posY, posZ;
    float velX, velY, velZ;
    int   age;
    int   lifeTime;
    bool  onGround;
    bool  isInWater;
    bool  removed;
    bool  dead;

    // 输出
    PhysicsResult result;
};

static void computePhysics(ItemSnapshot& snap) {
    if (snap.removed || snap.dead) {
        snap.result.shouldRemove = false;
        snap.result.newPosX = snap.posX;
        snap.result.newPosY = snap.posY;
        snap.result.newPosZ = snap.posZ;
        snap.result.newVelX = snap.velX;
        snap.result.newVelY = snap.velY;
        snap.result.newVelZ = snap.velZ;
        snap.result.newAge  = snap.age;
        return;
    }

    float vx = snap.velX;
    float vy = snap.velY;
    float vz = snap.velZ;

    // 重力
    vy += GRAVITY;

    // 水中浮力近似
    if (snap.isInWater) {
        vy += 0.06f;  // 浮力抵消部分重力
        vx *= 0.95f;
        vy *= 0.95f;
        vz *= 0.95f;
    }

    // 空气阻力
    vx *= DRAG_AIR;
    vy *= DRAG_AIR;
    vz *= DRAG_AIR;

    // 地面摩擦
    if (snap.onGround) {
        vx *= DRAG_GROUND;
        vz *= DRAG_GROUND;
        // 地面上 Y 速度归零（防止穿地）
        if (vy < 0.0f) vy = 0.0f;
    }

    // 位置更新
    float nx = snap.posX + vx;
    float ny = snap.posY + vy;
    float nz = snap.posZ + vz;

    // 年龄
    int newAge = snap.age + 1;

    // despawn 检查
    bool shouldRemove = false;
    if (newAge >= MAX_AGE && snap.lifeTime <= 0) {
        shouldRemove = true;
    }
    if (newAge >= MAX_LIFETIME) {
        shouldRemove = true;
    }

    snap.result.newPosX      = nx;
    snap.result.newPosY      = ny;
    snap.result.newPosZ      = nz;
    snap.result.newVelX      = vx;
    snap.result.newVelY      = vy;
    snap.result.newVelZ      = vz;
    snap.result.newAge       = newAge;
    snap.result.shouldRemove = shouldRemove;
}

// ============================================================
// Hook: Actor::tick — 跳过已处理的 ItemActor
// ============================================================
LL_TYPE_INSTANCE_HOOK(
    ActorTickHook,
    ll::memory::HookPriority::Normal,
    Actor,
    &Actor::tick,
    bool,
    ::BlockSource& region
) {
    if (gSkipProcessedItems && dynamic_cast<ItemActor*>(this) != nullptr) {
        return false;
    }
    return origin(region);
}

// ============================================================
// Hook: Level::$tick — 主逻辑
//
// 策略：
// 阶段 1（主线程）：快照所有 ItemActor 的物理状态
// 阶段 2（并行）  ：纯数学计算物理更新，零 BDS API 调用
// 阶段 3（主线程）：应用物理结果 + 原版 tick 处理交互
// ============================================================
LL_TYPE_INSTANCE_HOOK(
    LevelTickHook,
    ll::memory::HookPriority::Normal,
    Level,
    &Level::$tick,
    void
) {
    if (!gConfig.enabled) {
        origin();
        return;
    }

    auto tickStart = std::chrono::steady_clock::now();

    // ── 阶段 1：快照 ──
    struct ItemRef {
        ItemActor*   actor;
        BlockSource* region;
    };

    std::vector<ItemRef>      refs;
    std::vector<ItemSnapshot> snapshots;
    refs.reserve(256);
    snapshots.reserve(256);

    auto actorList = getRuntimeActorList();
    for (Actor* actor : actorList) {
        if (!actor || actor->mRemoved) continue;
        auto* itemActor = dynamic_cast<ItemActor*>(actor);
        if (!itemActor) continue;

        BlockSource& bs = actor->getDimensionBlockSource();

        auto const& pos = actor->getPosition();
        auto const& vel = actor->getPosDelta();

        ItemSnapshot snap{};
        snap.posX     = pos.x;
        snap.posY     = pos.y;
        snap.posZ     = pos.z;
        snap.velX     = vel.x;
        snap.velY     = vel.y;
        snap.velZ     = vel.z;
        snap.age      = itemActor->age();
        snap.lifeTime = itemActor->lifeTime();
        snap.onGround = actor->isOnGround();
        snap.isInWater = actor->isInWater();
        snap.removed  = actor->mRemoved;
        snap.dead     = actor->isDead();

        refs.push_back({itemActor, &bs});
        snapshots.push_back(snap);
    }

    size_t count = refs.size();

    if (count < static_cast<size_t>(gConfig.minParallelCnt)) {
        origin();
        if (gConfig.debug) {
            getLogger().info("Skipped parallel (count={} < min={})", count, gConfig.minParallelCnt);
        }
        return;
    }

    // ── 阶段 2：并行物理计算（纯数学，零 BDS 调用）──
    gWorkerPool->parallelFor(count, [&snapshots](size_t i) {
        computePhysics(snapshots[i]);
    });

    // ── 阶段 3：主线程串行应用结果 ──
    for (size_t i = 0; i < count; i++) {
        auto& ref  = refs[i];
        auto& res  = snapshots[i].result;
        auto* item = ref.actor;

        if (item->mRemoved || item->isDead()) continue;

        if (res.shouldRemove) {
            item->remove();
            continue;
        }

        // 应用物理结果
        // 使用 Actor 的 API 在主线程设置位置和速度
        item->setPosDelta(Vec3{res.newVelX, res.newVelY, res.newVelZ});
        item->age() = res.newAge;

        // 调用原版 tick 处理碰撞检测、方块交互、拾取等
        // 但跳过物理更新部分（速度已经预计算好了）
        item->tick(*ref.region);
    }

    // ── 串行 merge ──
    size_t mergeCount = 0;
    for (size_t i = 0; i < count; i++) {
        auto* item = refs[i].actor;
        if (item->mRemoved || item->isDead()) continue;
        try {
            item->_mergeWithNeighbours();
            mergeCount++;
        } catch (...) {}
    }

    // ── 原版 tick，跳过已处理的 ItemActor ──
    gSkipProcessedItems = true;
    origin();
    gSkipProcessedItems = false;

    // ── 统计 ──
    auto elapsedUs = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - tickStart
        ).count()
    );

    gTotalTicks.fetch_add(1, std::memory_order_relaxed);
    gTotalProcessed.fetch_add(count, std::memory_order_relaxed);
    gTotalMerged.fetch_add(mergeCount, std::memory_order_relaxed);
    gTotalTimeUs.fetch_add(elapsedUs, std::memory_order_relaxed);

    uint64_t curMax = gMaxTimeUs.load(std::memory_order_relaxed);
    while (elapsedUs > curMax &&
           !gMaxTimeUs.compare_exchange_weak(curMax, elapsedUs, std::memory_order_relaxed))
        ;

    if (gConfig.debug) {
        getLogger().info("Tick: {} items, {:.2f}ms", count, elapsedUs / 1000.0);
    }
}

// ============================================================
// 插件主类
// ============================================================
ParallelItemTickMod& ParallelItemTickMod::getInstance() {
    static ParallelItemTickMod instance;
    return instance;
}

bool ParallelItemTickMod::load() {
    std::filesystem::create_directories(getSelf().getConfigDir());
    if (!loadConfig()) {
        getLogger().warn("Failed to load config, using defaults");
        saveConfig();
    }
    getLogger().info(
        "Loaded. enabled={}, debug={}, stats={}, workers={}, minCnt={}",
        gConfig.enabled, gConfig.debug, gConfig.stats,
        gConfig.workerThreads, gConfig.minParallelCnt
    );
    return true;
}

bool ParallelItemTickMod::enable() {
    if (!gConfig.enabled) {
        getLogger().info("Plugin disabled by config.");
        return true;
    }

    gWorkerPool = std::make_unique<TickWorkerPool>(gConfig.workerThreads);

    ActorTickHook::hook();
    LevelTickHook::hook();

    if (gConfig.stats) startStatsTask();

    getLogger().info(
        "Enabled with {} workers, parallel physics compute",
        gConfig.workerThreads
    );
    return true;
}

bool ParallelItemTickMod::disable() {
    stopStatsTask();

    ActorTickHook::unhook();
    LevelTickHook::unhook();

    gWorkerPool.reset();
    gSkipProcessedItems = false;

    getLogger().info("Disabled");
    return true;
}

} // namespace parallel_item_tick

LL_REGISTER_MOD(
    parallel_item_tick::ParallelItemTickMod,
    parallel_item_tick::ParallelItemTickMod::getInstance()
);
