// ParallelItemTick.cpp
#include "ParallelItemTick.h"

#include <chrono>
#include <filesystem>
#include <latch>
#include <vector>

#include <ll/api/Config.h>
#include <ll/api/coro/CoroTask.h>
#include <ll/api/io/Logger.h>
#include <ll/api/io/LoggerRegistry.h>
#include <ll/api/memory/Hook.h>
#include <ll/api/mod/NativeMod.h>
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
bool                            gStatsRunning    = false;
std::unique_ptr<TickWorkerPool> gWorkerPool;

thread_local bool gSuppressMerge      = false;
bool              gSkipProcessedItems = false;

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
    auto path = ll::mod::NativeMod::current()->getConfigDir() / "config.json";
    if (!std::filesystem::exists(path)) {
        getLogger().info("Config not found, creating default config.json");
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
    auto path = ll::mod::NativeMod::current()->getConfigDir() / "config.json";
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

                auto totalTicks   = gTotalTicks.load(std::memory_order_relaxed);
                auto totalProcess = gTotalProcessed.load(std::memory_order_relaxed);
                auto totalMerged  = gTotalMerged.load(std::memory_order_relaxed);
                auto totalTime    = gTotalTimeUs.load(std::memory_order_relaxed);
                auto maxTime      = gMaxTimeUs.load(std::memory_order_relaxed);

                getLogger().info(
                    "Stats (5s): frames={}, items/frame={:.1f}, merges/frame={:.1f}, "
                    "avgTime={:.2f}ms, maxTime={:.2f}ms",
                    totalTicks,
                    totalTicks ? (double)totalProcess / totalTicks : 0.0,
                    totalTicks ? (double)totalMerged  / totalTicks : 0.0,
                    totalTicks ? (double)totalTime    / totalTicks / 1000.0 : 0.0,
                    (double)maxTime / 1000.0
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
        mWorkers.emplace_back([this, i] { workerMain(i); });
    }
}

TickWorkerPool::~TickWorkerPool() {
    mShutdown.store(true, std::memory_order_release);
    mGeneration.fetch_add(1, std::memory_order_release);
    mGeneration.notify_all();
    for (auto& w : mWorkers) {
        if (w.joinable()) w.join();
    }
}

void TickWorkerPool::parallelFor(size_t count, std::function<void(size_t)> const& func) {
    if (count == 0) return;
    if (count < mMinParallelThreshold) {
        for (size_t i = 0; i < count; i++) func(i);
        return;
    }

    mWorkFunc  = &func;
    mWorkCount = count;
    mNextIndex.store(0, std::memory_order_release);

    std::latch done((ptrdiff_t)(mNumWorkers + 1));
    mDoneLatch = &done;

    mGeneration.fetch_add(1, std::memory_order_release);
    mGeneration.notify_all();

    executeWork();
    done.count_down();
    done.wait();

    mDoneLatch = nullptr;
    mWorkFunc  = nullptr;
}

void TickWorkerPool::workerMain(int) {
    uint64_t lastGen = 0;
    while (true) {
        mGeneration.wait(lastGen, std::memory_order_acquire);
        if (mShutdown.load(std::memory_order_acquire)) return;
        lastGen = mGeneration.load(std::memory_order_acquire);
        executeWork();
        if (mDoneLatch) mDoneLatch->count_down();
    }
}

void TickWorkerPool::executeWork() {
    auto const& func = *mWorkFunc;
    while (true) {
        size_t idx = mNextIndex.fetch_add(1, std::memory_order_acq_rel);
        if (idx >= mWorkCount) break;
        func(idx);
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
// 用 dynamic_cast 判断是否为 ItemActor，避免使用 ActorType 枚举
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
// Hook 3: Level::$tick
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

    struct ItemEntry {
        ItemActor*   actor;
        BlockSource* region;
    };
    std::vector<ItemEntry> items;
    items.reserve(256);

    // 遍历所有 Actor，过滤出 ItemActor
    auto actorList = getRuntimeActorList();
    for (Actor* actor : actorList) {
        if (!actor) continue;
        if (actor->mRemoved) continue;
        // 用 dynamic_cast 安全判断类型，不依赖 ActorType 枚举
        auto* itemActor = dynamic_cast<ItemActor*>(actor);
        if (!itemActor) continue;
        BlockSource& bs = actor->getDimensionBlockSource();
        items.push_back({ itemActor, &bs });
    }

    size_t count = items.size();

    if (count < (size_t)gConfig.minParallelCnt) {
        // 数量不足，走原版逻辑
        origin();
        if (gConfig.debug) {
            getLogger().info("Skipped parallel (count={} < min={})", count, gConfig.minParallelCnt);
        }
        return;
    }

    // 阶段 1: 并行 tick（抑制 merge）
    gWorkerPool->parallelFor(count, [&items](size_t i) {
        gSuppressMerge = true;
        items[i].actor->tick(*items[i].region);
        gSuppressMerge = false;
    });

    // 阶段 2: 串行 merge
    for (auto& e : items) {
        if (e.actor->mRemoved || e.actor->isDead()) continue;
        e.actor->_mergeWithNeighbours();
    }

    // 阶段 3: 原版 tick，ActorTickHook 跳过已处理的 ItemActor
    gSkipProcessedItems = true;
    origin();
    gSkipProcessedItems = false;

    auto elapsedUs = (uint64_t)std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - tickStart
    ).count();

    gTotalTicks.fetch_add(1, std::memory_order_relaxed);
    gTotalProcessed.fetch_add(count, std::memory_order_relaxed);
    gTotalTimeUs.fetch_add(elapsedUs, std::memory_order_relaxed);

    uint64_t curMax = gMaxTimeUs.load(std::memory_order_relaxed);
    while (elapsedUs > curMax &&
           !gMaxTimeUs.compare_exchange_weak(curMax, elapsedUs, std::memory_order_relaxed));

    if (gConfig.debug) {
        getLogger().info("Tick processed {} items in {:.2f}ms", count, elapsedUs / 1000.0);
    }
}

// ============================================================
// 插件主类实现
// ============================================================
ParallelItemTickMod& ParallelItemTickMod::getInstance() {
    static ParallelItemTickMod instance;
    return instance;
}

ParallelItemTickMod::ParallelItemTickMod() : mSelf(*ll::mod::NativeMod::current()) {}

ll::mod::NativeMod& ParallelItemTickMod::getSelf() const { return mSelf; }

bool ParallelItemTickMod::load() {
    std::filesystem::create_directories(mSelf.getConfigDir());
    if (!loadConfig()) {
        getLogger().warn("Failed to load config, using defaults");
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
        "Enabled with {} workers, minParallelCnt={}",
        gConfig.workerThreads, gConfig.minParallelCnt
    );
    return true;
}

bool ParallelItemTickMod::disable() {
    stopStatsTask();

    MergeWithNeighboursHook::unhook();
    ActorTickHook::unhook();
    LevelTickHook::unhook();

    gWorkerPool.reset();
    gSkipProcessedItems = false;

    getLogger().info("Disabled");
    return true;
}

} // namespace parallel_item_tick

// ============================================================
// 注册插件 - LL_REGISTER_MOD 只接受类名
// ============================================================
LL_REGISTER_MOD(parallel_item_tick::ParallelItemTickMod);
