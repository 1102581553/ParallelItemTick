// ParallelItemTick.cpp

#include "ParallelItemTick.h"

#include <ll/api/memory/Hook.h>
#include <ll/api/mod/RegisterHelper.h>
#include <ll/api/io/Logger.h>
#include <ll/api/io/LoggerRegistry.h>
#include <ll/api/Config.h>
#include <ll/api/coro/CoroTask.h>
#include <ll/api/thread/ServerThreadExecutor.h>

#include <mc/world/actor/Actor.h>
#include <mc/world/actor/item/ItemActor.h>
#include <mc/world/level/BlockSource.h>
#include <mc/world/level/Level.h>
#include <mc/world/level/Tick.h>
#include <mc/deps/ecs/gamerefs_entity/EntityContext.h>
#include <mc/deps/vanilla_components/ItemActorFlagComponent.h>
#include <mc/entity/components/ActorTickNeededComponent.h>
#include <mc/entity/systems/ActorLegacyTickSystem.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <latch>
#include <mutex>
#include <thread>
#include <vector>
#include <algorithm>

namespace parallel_item_tick {

// ============================================================
// 持久线程池
// ============================================================
class TickWorkerPool {
public:
    explicit TickWorkerPool(int numWorkers) : mNumWorkers(numWorkers) {
        for (int i = 0; i < numWorkers; i++) {
            mWorkers.emplace_back([this] { workerMain(); });
        }
    }

    ~TickWorkerPool() {
        mShutdown.store(true, std::memory_order_release);
        mGeneration.fetch_add(1, std::memory_order_release);
        mGeneration.notify_all();
        for (auto& w : mWorkers) {
            if (w.joinable()) w.join();
        }
    }

    TickWorkerPool(TickWorkerPool const&)            = delete;
    TickWorkerPool& operator=(TickWorkerPool const&) = delete;

    void parallelFor(size_t count, std::function<void(size_t)> const& func) {
        if (count == 0) return;

        mWorkFunc  = &func;
        mWorkCount = count;
        mNextIndex.store(0, std::memory_order_release);

        std::latch done(mNumWorkers + 1);
        mDoneLatch = &done;

        mGeneration.fetch_add(1, std::memory_order_release);
        mGeneration.notify_all();

        executeWork();
        done.count_down();
        done.wait();

        mDoneLatch = nullptr;
        mWorkFunc  = nullptr;
    }

private:
    void workerMain() {
        uint64_t lastGen = mGeneration.load(std::memory_order_acquire);
        while (true) {
            mGeneration.wait(lastGen, std::memory_order_acquire);
            lastGen = mGeneration.load(std::memory_order_acquire);
            if (mShutdown.load(std::memory_order_acquire)) return;

            executeWork();
            if (mDoneLatch) mDoneLatch->count_down();
        }
    }

    void executeWork() {
        if (!mWorkFunc) return;
        auto const& func = *mWorkFunc;
        while (true) {
            size_t idx = mNextIndex.fetch_add(1, std::memory_order_acq_rel);
            if (idx >= mWorkCount) break;
            func(idx);
        }
    }

    int                                    mNumWorkers;
    std::vector<std::thread>               mWorkers;
    std::atomic<bool>                      mShutdown{false};
    std::atomic<uint64_t>                  mGeneration{0};
    std::function<void(size_t)> const*     mWorkFunc{nullptr};
    size_t                                 mWorkCount{0};
    std::atomic<size_t>                    mNextIndex{0};
    std::latch*                            mDoneLatch{nullptr};
};

// ============================================================
// 全局状态
// ============================================================
static Config                          config;
static std::shared_ptr<ll::io::Logger> logger;
static std::unique_ptr<TickWorkerPool> workerPool;
static bool                            debugTaskRunning = false;

// 并行阶段标记
static thread_local bool gSuppressMerge = false;

// 跳过标记
static bool gSkipProcessedItems = false;

// 统计（只在主线程读写，stats 输出用 atomic 防万一）
static std::atomic<uint64_t> statParallelTicks{0};
static std::atomic<uint64_t> statItemsProcessed{0};
static std::atomic<uint64_t> statItemsMerged{0};
static std::atomic<uint64_t> statItemsSkippedDead{0};
static std::atomic<uint64_t> statFallbackSerial{0};

// 实时 debug 数据（每帧更新，主线程写）
static size_t debugLastFrameItemCount  = 0;
static double debugLastFrameParallelMs = 0.0;
static double debugLastFrameMergeMs    = 0.0;

// ============================================================
// Logger
// ============================================================
static ll::io::Logger& getLogger() {
    if (!logger) {
        logger = ll::io::LoggerRegistry::getInstance().getOrCreate("ParallelItemTick");
    }
    return *logger;
}

// ============================================================
// Config
// ============================================================
Config& getConfig() { return config; }

bool loadConfig() {
    auto path   = ParallelItemTick::getInstance().getSelf().getConfigDir() / "config.json";
    bool loaded = ll::config::loadConfig(config, path);
    if (config.workerThreads <= 0) {
        config.workerThreads = std::clamp(
            (int)std::thread::hardware_concurrency() - 1, 1, 7
        );
    }
    config.workerThreads    = std::clamp(config.workerThreads, 1, 7);
    config.minParallelCount = std::max(config.minParallelCount, 4);
    config.statsIntervalSec = std::clamp(config.statsIntervalSec, 1, 60);
    return loaded;
}

bool saveConfig() {
    auto path = ParallelItemTick::getInstance().getSelf().getConfigDir() / "config.json";
    return ll::config::saveConfig(config, path);
}

// ============================================================
// Stats 定时输出
// ============================================================
static void resetStats() {
    statParallelTicks.store(0, std::memory_order_relaxed);
    statItemsProcessed.store(0, std::memory_order_relaxed);
    statItemsMerged.store(0, std::memory_order_relaxed);
    statItemsSkippedDead.store(0, std::memory_order_relaxed);
    statFallbackSerial.store(0, std::memory_order_relaxed);
}

static void startStatsTask() {
    if (debugTaskRunning) return;
    debugTaskRunning = true;

    ll::coro::keepThis([]() -> ll::coro::CoroTask<> {
        while (debugTaskRunning) {
            co_await std::chrono::seconds(config.statsIntervalSec);
            ll::thread::ServerThreadExecutor::getDefault().execute([] {
                if (!debugTaskRunning) return;

                // stats 输出（默认开启）
                if (config.stats) {
                    auto ticks     = statParallelTicks.load(std::memory_order_relaxed);
                    auto processed = statItemsProcessed.load(std::memory_order_relaxed);
                    auto merged    = statItemsMerged.load(std::memory_order_relaxed);
                    auto dead      = statItemsSkippedDead.load(std::memory_order_relaxed);
                    auto fallback  = statFallbackSerial.load(std::memory_order_relaxed);
                    double avgItems = ticks > 0 ? (double)processed / ticks : 0.0;

                    getLogger().info(
                        "[Stats {}s] ticks={}, items={}, avg={:.1f}/tick, "
                        "merged={}, deadSkip={}, serialFallback={}",
                        config.statsIntervalSec,
                        ticks, processed, avgItems,
                        merged, dead, fallback
                    );
                    resetStats();
                }

                // debug 实时输出（需要手动开启）
                if (config.debug) {
                    getLogger().info(
                        "[Debug] lastFrame: items={}, parallelMs={:.2f}, mergeMs={:.2f}",
                        debugLastFrameItemCount,
                        debugLastFrameParallelMs,
                        debugLastFrameMergeMs
                    );
                }
            });
        }
        debugTaskRunning = false;
    }).launch(ll::thread::ServerThreadExecutor::getDefault());
}

static void stopStatsTask() { debugTaskRunning = false; }

// ============================================================
// 插件生命周期
// ============================================================
ParallelItemTick& ParallelItemTick::getInstance() {
    static ParallelItemTick instance;
    return instance;
}

bool ParallelItemTick::load() {
    std::filesystem::create_directories(getSelf().getConfigDir());
    if (!loadConfig()) {
        getLogger().warn("Config not found or invalid, saving defaults");
        saveConfig();
    }
    getLogger().info(
        "Loaded. enabled={}, debug={}, stats={}, workers={}, minParallel={}",
        config.enabled, config.debug, config.stats,
        config.workerThreads, config.minParallelCount
    );
    return true;
}

bool ParallelItemTick::enable() {
    if (!config.enabled) {
        getLogger().info("Plugin disabled by config");
        return true;
    }

    workerPool = std::make_unique<TickWorkerPool>(config.workerThreads);
    getLogger().info("Worker pool created with {} threads", config.workerThreads);

    if (config.stats || config.debug) {
        startStatsTask();
    }

    return true;
}

bool ParallelItemTick::disable() {
    stopStatsTask();
    workerPool.reset();
    resetStats();
    debugLastFrameItemCount  = 0;
    debugLastFrameParallelMs = 0.0;
    debugLastFrameMergeMs    = 0.0;
    getLogger().info("Disabled, worker pool destroyed");
    return true;
}

} // namespace parallel_item_tick

// ============================================================
// Hook 1: ItemActor::_mergeWithNeighbours
//         并行阶段 no-op，串行阶段正常执行
// ============================================================
LL_AUTO_TYPE_INSTANCE_HOOK(
    ItemMergeHook,
    ll::memory::HookPriority::Normal,
    ItemActor,
    &ItemActor::_mergeWithNeighbours,
    void
) {
    if (parallel_item_tick::gSuppressMerge) return;
    origin();
}

// ============================================================
// Hook 2: ActorLegacyTickSystem::tickActorLegacyTickSystem
//         跳过已被并行处理的 ItemActor
// ============================================================
LL_AUTO_STATIC_HOOK(
    PerEntityTickHook,
    ll::memory::HookPriority::Normal,
    ActorLegacyTickSystem::tickActorLegacyTickSystem,
    void,
    ::EntityContext&             entity,
    ::Actor&                    actor,
    ::ActorTickNeededComponent& tickComp
) {
    if (parallel_item_tick::gSkipProcessedItems
        && entity.hasComponent<ItemActorFlagComponent>())
    {
        return;
    }
    origin(entity, actor, tickComp);
}

// ============================================================
// Hook 3: ActorLegacyTickSystem::tick
//         在原版 tick 之前并行处理所有掉落物
// ============================================================
LL_AUTO_TYPE_INSTANCE_HOOK(
    SystemTickHook,
    ll::memory::HookPriority::Normal,
    ActorLegacyTickSystem,
    &ActorLegacyTickSystem::tick,
    void,
    ::EntityRegistry& registry
) {
    using namespace parallel_item_tick;

    if (!config.enabled || !workerPool) {
        origin(registry);
        return;
    }

    // ---- Phase 0: 收集 ----
    struct ItemTickEntry {
        ItemActor*   actor;
        BlockSource* region;
    };

    std::vector<ItemTickEntry> items;
    items.reserve(256);

    auto& enttRegistry = registry.getEnTTRegistry();
    auto  view = enttRegistry.view<ItemActorFlagComponent, ActorTickNeededComponent>();

    for (auto entity : view) {
        auto& tickComp = view.get<ActorTickNeededComponent>(entity);

        EntityContext ctx(registry, entity);
        Actor* actor = Actor::tryGetFromEntity(ctx, false);
        if (!actor) continue;

        // WeakRef<BlockSource>::lock() 返回的是可以隐式转换为指针的类型
        auto regionLock = tickComp.mBlockSource.lock();
        if (!regionLock) continue;

        items.push_back({
            static_cast<ItemActor*>(actor),
            &*regionLock
        });
    }

    // 数量不够，走原版串行
    if ((int)items.size() < config.minParallelCount) {
        statFallbackSerial.fetch_add(1, std::memory_order_relaxed);
        debugLastFrameItemCount  = items.size();
        debugLastFrameParallelMs = 0.0;
        debugLastFrameMergeMs    = 0.0;
        origin(registry);
        return;
    }

    // ---- Phase 1: 并行物理 Tick ----
    auto t0 = std::chrono::steady_clock::now();

    workerPool->parallelFor(items.size(), [&items](size_t i) {
        gSuppressMerge = true;
        items[i].actor->tick(*items[i].region);
        gSuppressMerge = false;
    });

    auto t1 = std::chrono::steady_clock::now();

    // ---- Phase 2: 串行合并 ----
    size_t mergedCount = 0;
    size_t deadCount   = 0;

    for (auto& entry : items) {
        if (entry.actor->isDead() || entry.actor->mRemoved) {
            deadCount++;
            continue;
        }
        entry.actor->_mergeWithNeighbours();
        mergedCount++;
    }

    auto t2 = std::chrono::steady_clock::now();

    // ---- Phase 3: 原版 tick 跳过 ItemActor ----
    gSkipProcessedItems = true;
    origin(registry);
    gSkipProcessedItems = false;

    // ---- 统计 ----
    statParallelTicks.fetch_add(1, std::memory_order_relaxed);
    statItemsProcessed.fetch_add(items.size(), std::memory_order_relaxed);
    statItemsMerged.fetch_add(mergedCount, std::memory_order_relaxed);
    statItemsSkippedDead.fetch_add(deadCount, std::memory_order_relaxed);

    debugLastFrameItemCount  = items.size();
    debugLastFrameParallelMs = std::chrono::duration<double, std::milli>(t1 - t0).count();
    debugLastFrameMergeMs    = std::chrono::duration<double, std::milli>(t2 - t1).count();

    // debug 实时输出（每帧）
    if (config.debug) {
        getLogger().debug(
            "[Tick] items={}, parallel={:.2f}ms, merge={:.2f}ms, dead={}",
            items.size(), debugLastFrameParallelMs, debugLastFrameMergeMs, deadCount
        );
    }
}

LL_REGISTER_MOD(
    parallel_item_tick::ParallelItemTick,
    parallel_item_tick::ParallelItemTick::getInstance()
);
