// ParallelItemTick.cpp
#include "ParallelItemTick.h"

#include <chrono>
#include <filesystem>
#include <unordered_set>
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

// 已被并行处理的 ItemActor 指针集合（主线程独占）
std::unordered_set<void*> gProcessedActors;

thread_local bool gSuppressMerge = false;

std::atomic<uint64_t> gTotalTicks{0};
std::atomic<uint64_t> gTotalProcessed{0};
std::atomic<uint64_t> gTotalMerged{0};
std::atomic<uint64_t> gTotalTimeUs{0};
std::atomic<uint64_t> gMaxTimeUs{0};

// ============================================================
// 日志
// ============================================================
ll::io::Logger& getLogger() {
    if (!gLogger) {
        gLogger = ll::io::LoggerRegistry::getInstance().getOrCreate("ParallelItemTick");
    }
    return *gLogger;
}

// ============================================================
// 配置
// ============================================================
bool loadConfig() {
    auto path = ParallelItemTickMod::getInstance().getSelf().getConfigDir() / "config.json";
    if (!std::filesystem::exists(path)) {
        getLogger().info("Config not found, creating default");
        return saveConfig();
    }
    if (!ll::config::loadConfig(gConfig, path)) {
        getLogger().warn("Failed to load config, using defaults");
    }
    if (gConfig.minParallelCnt < 1) gConfig.minParallelCnt = 16;
    if (gConfig.workerThreads  < 0) gConfig.workerThreads  = 0;
    if (gConfig.workerThreads == 0) {
        int cores             = (int)std::thread::hardware_concurrency();
        gConfig.workerThreads = std::max(1, std::min(cores - 1, 7));
    }
    return true;
}

bool saveConfig() {
    auto path = ParallelItemTickMod::getInstance().getSelf().getConfigDir() / "config.json";
    return ll::config::saveConfig(gConfig, path);
}

// ============================================================
// 统计任务
// ============================================================
void startStatsTask() {
    if (gStatsRunning || !gConfig.stats) return;
    gStatsRunning = true;

    ll::coro::keepThis([]() -> ll::coro::CoroTask<> {
        while (gStatsRunning) {
            co_await std::chrono::seconds(5);
            ll::thread::ServerThreadExecutor::getDefault().execute([] {
                if (!gConfig.stats) return;

                auto ticks   = gTotalTicks.load(std::memory_order_relaxed);
                auto process = gTotalProcessed.load(std::memory_order_relaxed);
                auto merged  = gTotalMerged.load(std::memory_order_relaxed);
                auto time    = gTotalTimeUs.load(std::memory_order_relaxed);
                auto maxT    = gMaxTimeUs.load(std::memory_order_relaxed);

                getLogger().info(
                    "Stats(5s): frames={}, items/f={:.1f}, merges/f={:.1f}, "
                    "avg={:.2f}ms, max={:.2f}ms",
                    ticks,
                    ticks ? (double)process / ticks : 0.0,
                    ticks ? (double)merged   / ticks : 0.0,
                    ticks ? (double)time     / ticks / 1000.0 : 0.0,
                    (double)maxT / 1000.0
                );

                gTotalTicks.store(0,     std::memory_order_relaxed);
                gTotalProcessed.store(0, std::memory_order_relaxed);
                gTotalMerged.store(0,    std::memory_order_relaxed);
                gTotalTimeUs.store(0,    std::memory_order_relaxed);
                gMaxTimeUs.store(0,      std::memory_order_relaxed);
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
    mCvWork.notify_all();
    for (auto& w : mWorkers) {
        if (w.joinable()) w.join();
    }
}

void TickWorkerPool::parallelFor(size_t count, std::function<void(size_t)> const& func) {
    if (count == 0) return;

    if (mNumWorkers == 0 || count < (size_t)gConfig.minParallelCnt) {
        for (size_t i = 0; i < count; i++) func(i);
        return;
    }

    {
        std::lock_guard lock(mMutex);
        mWorkFunc     = &func;
        mWorkCount    = count;
        mNextIndex.store(0, std::memory_order_relaxed);
        mActiveWorkers = mNumWorkers;
    }
    mCvWork.notify_all();

    // 主线程也参与执行
    while (true) {
        size_t idx = mNextIndex.fetch_add(1, std::memory_order_acq_rel);
        if (idx >= count) break;
        func(idx);
    }

    // 等待所有工作线程完成
    std::unique_lock lock(mMutex);
    mCvDone.wait(lock, [this] { return mActiveWorkers == 0; });
    mWorkFunc = nullptr;
}

void TickWorkerPool::workerMain() {
    while (true) {
        std::unique_lock lock(mMutex);
        mCvWork.wait(lock, [this] {
            return mShutdown || mWorkFunc != nullptr;
        });
        if (mShutdown) return;

        auto const& func  = *mWorkFunc;
        size_t      count = mWorkCount;
        lock.unlock();

        while (true) {
            size_t idx = mNextIndex.fetch_add(1, std::memory_order_acq_rel);
            if (idx >= count) break;
            func(idx);
        }

        {
            std::lock_guard lg(mMutex);
            if (--mActiveWorkers == 0) {
                mCvDone.notify_one();
            }
        }
    }
}

// ============================================================
// Hook 1: ItemActor::_mergeWithNeighbours
// ============================================================
LL_TYPE_INSTANCE_HOOK(
    MergeWithNeighboursHook,
    ll::memory::HookPriority::Normal,
    ItemActor,
    &ItemActor::_mergeWithNeighbours,
    void
) {
    if (gSuppressMerge) return;
    origin();
    gTotalMerged.fetch_add(1, std::memory_order_relaxed);
}

// ============================================================
// Hook 2: Actor::tick
// 绝对不用 dynamic_cast！用指针集合判断是否跳过
// ============================================================
LL_TYPE_INSTANCE_HOOK(
    ActorTickHook,
    ll::memory::HookPriority::Normal,
    Actor,
    &Actor::tick,
    bool,
    ::BlockSource& region
) {
    // gProcessedActors 只在主线程的 phase3 期间被读取
    // Actor::tick 也在主线程调用，所以无需加锁
    if (!gProcessedActors.empty()) {
        if (gProcessedActors.count(static_cast<void*>(this))) {
            return false;
        }
    }
    return origin(region);
}

// ============================================================
// Hook 3: Level::$tick
// ============================================================
LL_TYPE_INSTANCE_HOOK(
    LevelTickHook,
    ll::memory::HookPriority::Normal,
    Level,
    &Level::$tick,
    void
) {
    if (!gConfig.enabled || !gWorkerPool) {
        origin();
        return;
    }

    auto tickStart = std::chrono::steady_clock::now();

    struct ItemEntry {
        ItemActor*   actor;
        BlockSource* region;
    };
    std::vector<ItemEntry> items;
    items.reserve(256);

    // 主线程收集 ItemActor，使用 hasCategory 或 getEntityTypeId
    // 不用 dynamic_cast，直接用 ActorCategory 判断
    auto actorList = getRuntimeActorList();
    for (Actor* actor : actorList) {
        if (!actor)          continue;
        if (actor->mRemoved) continue;
        // ItemActor 的 ActorCategory 包含 Item
        // 用 hasCategory 是纯虚函数调用，安全
        if (!actor->hasCategory(::ActorCategory::Item)) continue;
        if (actor->isDead()) continue;

        BlockSource& bs = actor->getDimensionBlockSource();
        // hasCategory 已确认是 ItemActor，static_cast 安全
        items.push_back({ static_cast<ItemActor*>(actor), &bs });
    }

    size_t count = items.size();

    if (count < (size_t)gConfig.minParallelCnt) {
        origin();
        if (gConfig.debug) {
            getLogger().info("Skipped parallel (count={} < min={})", count, gConfig.minParallelCnt);
        }
        return;
    }

    // ── 阶段1: 并行 tick（抑制 merge）──
    gWorkerPool->parallelFor(count, [&items](size_t i) {
        gSuppressMerge = true;
        items[i].actor->tick(*items[i].region);
        gSuppressMerge = false;
    });

    // ── 阶段2: 串行 merge + 建立跳过集合 ──
    gProcessedActors.clear();
    gProcessedActors.reserve(count);
    for (auto& e : items) {
        if (e.actor->mRemoved || e.actor->isDead()) continue;
        e.actor->_mergeWithNeighbours();
        // 记录已处理的指针，让 ActorTickHook 跳过
        gProcessedActors.insert(static_cast<void*>(e.actor));
    }

    // ── 阶段3: 原版 tick（ActorTickHook 查集合跳过已处理的）──
    origin();

    // 清理集合
    gProcessedActors.clear();

    auto elapsedUs = (uint64_t)std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - tickStart
    ).count();

    gTotalTicks.fetch_add(1,         std::memory_order_relaxed);
    gTotalProcessed.fetch_add(count, std::memory_order_relaxed);
    gTotalTimeUs.fetch_add(elapsedUs, std::memory_order_relaxed);

    uint64_t curMax = gMaxTimeUs.load(std::memory_order_relaxed);
    while (elapsedUs > curMax &&
           !gMaxTimeUs.compare_exchange_weak(curMax, elapsedUs, std::memory_order_relaxed));

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

    MergeWithNeighboursHook::hook();
    ActorTickHook::hook();
    LevelTickHook::hook();

    if (gConfig.stats) startStatsTask();

    getLogger().info(
        "Enabled. workers={}, minParallelCnt={}",
        gConfig.workerThreads, gConfig.minParallelCnt
    );
    return true;
}

bool ParallelItemTickMod::disable() {
    stopStatsTask();

    // 先 unhook 再销毁线程池
    MergeWithNeighboursHook::unhook();
    ActorTickHook::unhook();
    LevelTickHook::unhook();

    gWorkerPool.reset();
    gProcessedActors.clear();

    getLogger().info("Disabled");
    return true;
}

} // namespace parallel_item_tick

LL_REGISTER_MOD(
    parallel_item_tick::ParallelItemTickMod,
    parallel_item_tick::ParallelItemTickMod::getInstance()
);
