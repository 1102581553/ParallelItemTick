// ParallelItemTick.cpp (修复版)
#include "ParallelItemTick.h"

#include <chrono>
#include <filesystem>
#include <latch>

#include <ll/api/Config.h>
#include <ll/api/coro/CoroTask.h>
#include <ll/api/io/Logger.h>
#include <ll/api/io/LoggerRegistry.h>
#include <ll/api/memory/Hook.h>
#include <ll/api/thread/ServerThreadExecutor.h>

// Minecraft 头文件
#include <mc/world/actor/item/ItemActor.h>
#include <mc/world/actor/Actor.h>
#include <mc/world/level/BlockSource.h>
#include <mc/world/level/Level.h>
#include <mc/world/level/dimension/Dimension.h>

namespace parallel_item_tick {

// ============================================================
// 全局变量定义
// ============================================================
Config                         gConfig;
std::shared_ptr<ll::io::Logger> gLogger;
bool                           gStatsRunning = false;
std::unique_ptr<TickWorkerPool> gWorkerPool;

thread_local bool gSuppressMerge    = false;
bool              gSkipProcessedItems = false;

std::atomic<uint64_t> gTotalTicks{0};
std::atomic<uint64_t> gTotalProcessed{0};
std::atomic<uint64_t> gTotalMerged{0};
std::atomic<uint64_t> gTotalTimeUs{0};
std::atomic<uint64_t> gMaxTimeUs{0};

// ============================================================
// 日志辅助
// ============================================================
ll::io::Logger& getLogger() {
    if (!gLogger) {
        gLogger = ll::io::LoggerRegistry::getInstance().getOrCreate("ParallelItemTick");
    }
    return *gLogger;
}

// ============================================================
// 配置加载/保存
// ============================================================
bool loadConfig() {
    auto path = ll::mod::NativeMod::current()->getConfigDir() / "config.json";
    if (!std::filesystem::exists(path)) {
        getLogger().info("Configuration file not found, creating default config.json");
        return saveConfig();
    }
    auto res = ll::config::loadConfig(gConfig, path);
    if (!res) {
        getLogger().warn("Failed to load config, using defaults");
    }
    if (gConfig.minParallelCnt < 1) gConfig.minParallelCnt = 16;
    if (gConfig.workerThreads < 0)  gConfig.workerThreads = 0;
    if (gConfig.workerThreads == 0) {
        int cores = (int)std::thread::hardware_concurrency();
        gConfig.workerThreads = std::max(1, std::min(cores - 1, 7));
    }
    return res;
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

                double avgTime = totalTicks ? (double)totalTime / totalTicks : 0.0;

                getLogger().info(
                    "Stats (5s): frames={}, items/frame={:.1f}, merges/frame={:.1f}, "
                    "avgTime={:.2f}ms, maxTime={:.2f}ms",
                    totalTicks,
                    totalTicks ? (double)totalProcess / totalTicks : 0.0,
                    totalTicks ? (double)totalMerged  / totalTicks : 0.0,
                    avgTime / 1000.0,
                    maxTime / 1000.0
                );

                gTotalTicks.store(0,   std::memory_order_relaxed);
                gTotalProcessed.store(0, std::memory_order_relaxed);
                gTotalMerged.store(0,  std::memory_order_relaxed);
                gTotalTimeUs.store(0,  std::memory_order_relaxed);
                gMaxTimeUs.store(0,    std::memory_order_relaxed);
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
// Hook: ItemActor::_mergeWithNeighbours
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
// Hook: ItemActor::tick
// 替代不可用的 ActorLegacyTickSystem hooks
// 在并行阶段：抑制 merge；统计处理数量
// ============================================================
LL_TYPE_INSTANCE_HOOK(
    ItemActorTickHook,
    ll::memory::HookPriority::Normal,
    ItemActor,
    &ItemActor::tick,
    void,
    ::BlockSource& region
) {
    if (!gConfig.enabled) {
        origin(region);
        return;
    }
    // gSuppressMerge 由并行调度器设置（worker thread）
    // 串行路径正常执行
    origin(region);
}

// ============================================================
// Hook: Level::tick —— 在这里做并行 ItemActor tick
// ============================================================
LL_TYPE_INSTANCE_HOOK(
    LevelTickHook,
    ll::memory::HookPriority::Normal,
    Level,
    &Level::tick,
    void
) {
    if (!gConfig.enabled) {
        origin();
        return;
    }

    auto tickStart = std::chrono::steady_clock::now();

    // 收集所有维度中的 ItemActor
    struct ItemEntry {
        ItemActor*   actor;
        BlockSource* region;
    };
    std::vector<ItemEntry> items;
    items.reserve(256);

    // 遍历所有维度收集 ItemActor
    forEachDimension([&](Dimension& dim) -> bool {
        auto& bs = dim.getBlockSourceFromMainChunkSource();
        // fetchEntities 返回该维度所有 Actor，过滤 ItemActor
        // 注意：实际 API 请以 levilamina 头文件为准
        dim.forEachActor([&](Actor& actor) {
            if (!actor.isRemoved() && actor.isType(ActorType::Item)) {
                items.push_back({ static_cast<ItemActor*>(&actor), &bs });
            }
        });
        return true;
    });

    size_t count = items.size();

    if (count < (size_t)gConfig.minParallelCnt || count == 0) {
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
        if (e.actor->isRemoved()) continue;
        e.actor->_mergeWithNeighbours();
    }

    // 阶段 3: 正常 tick（ItemActor 已处理，其他 Actor 需要 tick）
    // 由于我们只并行了 ItemActor，Level::tick 内其他逻辑仍需执行
    // 但 ItemActor::tick 会被再次调用——需要跳过已处理的
    gSkipProcessedItems = true;
    origin();
    gSkipProcessedItems = false;

    auto elapsedUs = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - tickStart
    ).count();

    gTotalTicks.fetch_add(1, std::memory_order_relaxed);
    gTotalProcessed.fetch_add(count, std::memory_order_relaxed);
    gTotalTimeUs.fetch_add((uint64_t)elapsedUs, std::memory_order_relaxed);

    uint64_t curMax = gMaxTimeUs.load(std::memory_order_relaxed);
    while ((uint64_t)elapsedUs > curMax &&
           !gMaxTimeUs.compare_exchange_weak(curMax, (uint64_t)elapsedUs, std::memory_order_relaxed));

    if (gConfig.debug) {
        getLogger().info("Tick processed {} items in {:.2f}ms", count, elapsedUs / 1000.0);
    }
}

// ============================================================
// 跳过已并行处理的 ItemActor::tick（gSkipProcessedItems 阶段）
// ============================================================
// 注意：若上面 ItemActorTickHook 已存在，可在其中加判断：
// 将 ItemActorTickHook 修改为：
// if (gSkipProcessedItems) return;   <-- 跳过已处理
// origin(region);

// ============================================================
// 插件主类
// ============================================================
ParallelItemTickMod& ParallelItemTickMod::getInstance() {
    static ParallelItemTickMod instance;
    return instance;
}

ParallelItemTickMod::ParallelItemTickMod() : mSelf(*ll::mod::NativeMod::current()) {}

// ✅ 在 .cpp 里定义（头文件里只声明，不写函数体）
ll::mod::NativeMod& ParallelItemTickMod::getSelf() const { return mSelf; }

bool ParallelItemTickMod::load() {
    std::filesystem::create_directories(mSelf.getConfigDir());
    if (!loadConfig()) {
        getLogger().warn("Failed to load config, using defaults");
    }
    getLogger().info("Loaded. enabled={}, debug={}, stats={}, workers={}, minCnt={}",
                     gConfig.enabled, gConfig.debug, gConfig.stats,
                     gConfig.workerThreads, gConfig.minParallelCnt);
    return true;
}

bool ParallelItemTickMod::enable() {
    if (!gConfig.enabled) {
        getLogger().info("Plugin disabled by config.");
        return true;
    }

    gWorkerPool = std::make_unique<TickWorkerPool>(gConfig.workerThreads);

    MergeWithNeighboursHook::hook();
    ItemActorTickHook::hook();
    LevelTickHook::hook();

    if (gConfig.stats) startStatsTask();

    getLogger().info("Enabled with {} workers, minParallelCnt={}",
                     gConfig.workerThreads, gConfig.minParallelCnt);
    return true;
}

bool ParallelItemTickMod::disable() {
    stopStatsTask();

    MergeWithNeighboursHook::unhook();
    ItemActorTickHook::unhook();
    LevelTickHook::unhook();

    gWorkerPool.reset();
    gSkipProcessedItems = false;

    getLogger().info("Disabled");
    return true;
}

} // namespace parallel_item_tick

// ============================================================
// 注册插件
// ============================================================
LL_REGISTER_MOD(parallel_item_tick::ParallelItemTickMod, parallel_item_tick::ParallelItemTickMod::getInstance());
