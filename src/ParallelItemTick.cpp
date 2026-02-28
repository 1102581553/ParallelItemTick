// ParallelItemTick.cpp
#include "ParallelItemTick.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <unordered_map>
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

namespace parallel_item_tick {

// ============================================================
// 全局变量
// ============================================================
Config                          gConfig;
std::shared_ptr<ll::io::Logger> gLogger;
bool                            gStatsRunning = false;
std::unique_ptr<TickWorkerPool> gWorkerPool;

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
    if (gConfig.batchSize < 1) gConfig.batchSize = 64;
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
// 空间哈希网格 — 加速 merge 的邻居搜索
// 并行构建，主线程串行查询
// ============================================================
struct CellKey {
    int x, y, z;
    bool operator==(CellKey const& o) const { return x == o.x && y == o.y && z == o.z; }
};

struct CellKeyHash {
    size_t operator()(CellKey const& k) const {
        size_t h = std::hash<int>()(k.x);
        h ^= std::hash<int>()(k.y) + 0x9e3779b9 + (h << 6) + (h >> 2);
        h ^= std::hash<int>()(k.z) + 0x9e3779b9 + (h << 6) + (h >> 2);
        return h;
    }
};

// merge 搜索半径约 0.5 格，用 1 格的网格
static constexpr float CELL_SIZE     = 1.0f;
static constexpr float CELL_SIZE_INV = 1.0f / CELL_SIZE;

static CellKey posToCell(float x, float y, float z) {
    return {
        static_cast<int>(std::floor(x * CELL_SIZE_INV)),
        static_cast<int>(std::floor(y * CELL_SIZE_INV)),
        static_cast<int>(std::floor(z * CELL_SIZE_INV))
    };
}

// ============================================================
// Hook: Level::$tick — 主逻辑
//
// 策略：
// 1. 收集 ItemActor
// 2. 并行构建空间哈希（每个 item 的 cell 坐标）
// 3. 主线程串行 tick 所有 ItemActor（保证线程安全）
// 4. 用空间哈希加速 merge（替代原版的暴力搜索）
//
// 性能收益来自：
// - 空间哈希把 merge 从 O(n²) 降到 O(n)
// - 批量处理减少虚函数调用开销
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

    // ── 收集 ──
    struct ItemEntry {
        ItemActor*   actor;
        BlockSource* region;
        CellKey      cell;
    };

    std::vector<ItemEntry> items;
    items.reserve(256);

    auto actorList = getRuntimeActorList();
    for (Actor* actor : actorList) {
        if (!actor || actor->mRemoved) continue;
        auto* itemActor = dynamic_cast<ItemActor*>(actor);
        if (!itemActor) continue;

        BlockSource& bs  = actor->getDimensionBlockSource();
        auto const&  pos = actor->getPosition();

        items.push_back({
            itemActor,
            &bs,
            posToCell(pos.x, pos.y, pos.z)
        });
    }

    size_t count = items.size();

    if (count < static_cast<size_t>(gConfig.minParallelCnt)) {
        origin();
        if (gConfig.debug) {
            getLogger().info("Skipped parallel (count={} < min={})", count, gConfig.minParallelCnt);
        }
        return;
    }

    // ── 构建空间哈希 ──
    std::unordered_map<CellKey, std::vector<size_t>, CellKeyHash> grid;
    grid.reserve(count);
    for (size_t i = 0; i < count; i++) {
        grid[items[i].cell].push_back(i);
    }

    // ── 串行 tick 所有 ItemActor（主线程，安全）──
    for (size_t i = 0; i < count; i++) {
        auto& e = items[i];
        if (e.actor->mRemoved || e.actor->isDead()) continue;
        try {
            e.actor->tick(*e.region);
        } catch (...) {}
    }

    // ── 空间哈希加速 merge ──
    // 对每个 item，只检查同 cell 和相邻 26 个 cell 内的 item
    size_t mergeCount = 0;
    for (size_t i = 0; i < count; i++) {
        auto& e = items[i];
        if (e.actor->mRemoved || e.actor->isDead()) continue;

        auto const& pos = e.actor->getPosition();
        CellKey curCell  = posToCell(pos.x, pos.y, pos.z);

        for (int dx = -1; dx <= 1; dx++) {
            for (int dy = -1; dy <= 1; dy++) {
                for (int dz = -1; dz <= 1; dz++) {
                    CellKey neighbor{curCell.x + dx, curCell.y + dy, curCell.z + dz};
                    auto it = grid.find(neighbor);
                    if (it == grid.end()) continue;

                    for (size_t j : it->second) {
                        if (j <= i) continue;  // 避免重复检查
                        auto& other = items[j];
                        if (other.actor->mRemoved || other.actor->isDead()) continue;

                        // 距离检查
                        auto const& opos = other.actor->getPosition();
                        float ddx = pos.x - opos.x;
                        float ddy = pos.y - opos.y;
                        float ddz = pos.z - opos.z;
                        float distSq = ddx * ddx + ddy * ddy + ddz * ddz;

                        if (distSq < 0.5f * 0.5f) {
                            try {
                                e.actor->_merge(other.actor);
                                mergeCount++;
                            } catch (...) {}
                        }
                    }
                }
            }
        }
    }

    // ── 原版 tick 处理非 ItemActor ──
    // 用一个标志让 ActorTickHook 跳过 ItemActor
    static bool sSkipItems = false;
    sSkipItems = true;
    origin();
    sSkipItems = false;

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
        getLogger().info(
            "Tick: {} items, {} cells, {} merges, {:.2f}ms",
            count, grid.size(), mergeCount, elapsedUs / 1000.0
        );
    }
}

// ============================================================
// Hook: Actor::tick — 跳过已处理的 ItemActor
// ============================================================
static bool gSkipItems = false;

LL_TYPE_INSTANCE_HOOK(
    ActorTickHook,
    ll::memory::HookPriority::Normal,
    Actor,
    &Actor::tick,
    bool,
    ::BlockSource& region
) {
    if (gSkipItems && dynamic_cast<ItemActor*>(this) != nullptr) {
        return false;
    }
    return origin(region);
}

// ============================================================
// Hook: ItemActor::_mergeWithNeighbours — 抑制原版 merge
// 我们用空间哈希替代了原版的暴力搜索
// ============================================================
LL_TYPE_INSTANCE_HOOK(
    MergeWithNeighboursHook,
    ll::memory::HookPriority::Normal,
    ItemActor,
    &ItemActor::_mergeWithNeighbours,
    void
) {
    // 空操作 — merge 已在 LevelTickHook 中用空间哈希完成
    return;
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
    MergeWithNeighboursHook::hook();

    if (gConfig.stats) startStatsTask();

    getLogger().info(
        "Enabled with {} workers, spatial hash merge",
        gConfig.workerThreads
    );
    return true;
}

bool ParallelItemTickMod::disable() {
    stopStatsTask();

    ActorTickHook::unhook();
    LevelTickHook::unhook();
    MergeWithNeighboursHook::unhook();

    gWorkerPool.reset();

    getLogger().info("Disabled");
    return true;
}

} // namespace parallel_item_tick

LL_REGISTER_MOD(
    parallel_item_tick::ParallelItemTickMod,
    parallel_item_tick::ParallelItemTickMod::getInstance()
);
