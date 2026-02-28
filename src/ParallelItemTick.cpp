// ParallelItemTick.cpp
#include "ParallelItemTick.h"

#include <chrono>
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
    auto path = ParallelItemTickMod::getInstance().getSelf().getConfigDir() / "config.json";
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
// TickWorkerPool — mutex+condvar 实现，无栈悬空问题
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

    // 任务太少直接串行，避免线程调度开销
    if (count < (size_t)gConfig.minParallelCnt || mNumWorkers == 0) {
        for (size_t i = 0; i < count; i++) func(i);
        return;
    }

    {
        std::lock_guard lock(mMutex);
        mWorkFunc      = &func;
        mWorkCount     = count;
        mNextIndex.store(0, std::memory_order_relaxed);
        mActiveworkers = mNumWorkers;  // 所有工作线程都要参与
    }
    mCvWork.notify_all(); // 唤醒所有工作线程

    // 主线程也参与工作，减少空等
    {
        auto const& f = func;
        while (true) {
            size_t idx = mNextIndex.fetch_add(1, std::memory_order_acq_rel);
            if (idx >= count) break;
            f(idx);
        }
    }

    // 等待所有工作线程完成
    std::unique_lock lock(mMutex);
    mCvDone.wait(lock, [this] { return mActiveworkers == 0; });
    mWorkFunc = nullptr;
}

void TickWorkerPool::workerMain() {
    while (true) {
        std::unique_lock lock(mMutex);
        // 等待有任务或关闭信号
        mCvWork.wait(lock, [this] {
            return mShutdown || mWorkFunc != nullptr;
        });

        if (mShutdown) return;

        // 取得任务引用（mWorkFunc 此时有效）
        auto const& func  = *mWorkFunc;
        size_t      count = mWorkCount;
        lock.unlock();

        // 执行任务
        while (true) {
            size_t idx = mNextIndex.fetch_add(1, std::memory_order_acq_rel);
            if (idx >= count) break;
            func(idx);
        }

        // 报告完成
        {
            std::lock_guard lg(mMutex);
            --mActiveworkers;
            if (mActiveworkers == 0) {
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
// Hook 2: Actor::tick — 跳过已并行处理的 ItemActor
// 用 hasCategory 或直接用 getEntityTypeId 判断，避免 dynamic_cast 开销
// ============================================================
LL_TYPE_INSTANCE_HOOK(
    ActorTickHook,
    ll::memory::HookPriority::Normal,
    Actor,
    &Actor::tick,
    bool,
    ::BlockSource& region
) {
    if (gSkipProcessedItems) {
        // 用 mRemoved + isType 判断，ItemActor 的 ActorType 枚举值为 64（ItemEntity）
        // 这里直接用 dynamic_cast 判断类型，只在串行阶段调用，不影响并行安全
        if (dynamic_cast<ItemActor*>(this) != nullptr) {
            return false;
        }
    }
    return origin(region);
}

// ============================================================
// Hook 3: Level::$tick — 主逻辑
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

    // 收集所有存活的 ItemActor
    // getRuntimeActorList() 在主线程调用，安全
    auto actorList = getRuntimeActorList();
    for (Actor* actor : actorList) {
        if (!actor)            continue;
        if (actor->mRemoved)   continue;
        // static_cast 尝试：ItemActor 是 Actor 的子类
        // 用 dynamic_cast 确保类型安全
        auto* itemActor = dynamic_cast<ItemActor*>(actor);
        if (!itemActor)        continue;
        if (itemActor->isDead()) continue;
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

    // ── 阶段1: 并行 tick（抑制 merge 防止并发修改邻居）──
    gWorkerPool->parallelFor(count, [&items](size_t i) {
        gSuppressMerge = true;  // thread_local，每个线程独立
        items[i].actor->tick(*items[i].region);
        gSuppressMerge = false;
    });

    // ── 阶段2: 串行 merge（主线程，安全）──
    for (auto& e : items) {
        if (e.actor->mRemoved || e.actor->isDead()) continue;
        e.actor->_mergeWithNeighbours();
    }

    // ── 阶段3: 原版 tick，跳过已处理的 ItemActor ──
    gSkipProcessedItems = true;
    origin();
    gSkipProcessedItems = false;

    auto elapsedUs = (uint64_t)std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - tickStart
    ).count();

    gTotalTicks.fetch_add(1,     std::memory_order_relaxed);
    gTotalProcessed.fetch_add(count, std::memory_order_relaxed);
    gTotalTimeUs.fetch_add(elapsedUs, std::memory_order_relaxed);

    // 更新最大耗时
    uint64_t curMax = gMaxTimeUs.load(std::memory_order_relaxed);
    while (elapsedUs > curMax &&
           !gMaxTimeUs.compare_exchange_weak(curMax, elapsedUs, std::memory_order_relaxed));

    if (gConfig.debug) {
        getLogger().info(
            "Tick: {} items, {:.2f}ms",
            count, elapsedUs / 1000.0
        );
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

    MergeWithNeighboursHook::unhook();
    ActorTickHook::unhook();
    LevelTickHook::unhook();

    // 先 unhook 再销毁线程池，避免线程池销毁时仍有 hook 调用
    gWorkerPool.reset();
    gSkipProcessedItems = false;

    getLogger().info("Disabled");
    return true;
}

} // namespace parallel_item_tick

LL_REGISTER_MOD(parallel_item_tick::ParallelItemTickMod, parallel_item_tick::ParallelItemTickMod::getInstance());
