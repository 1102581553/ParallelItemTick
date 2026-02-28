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

// Minecraft 必要头文件
#include <mc/deps/ecs/gamerefs_entity/EntityRegistry.h>
#include <mc/deps/ecs/gamerefs_entity/EntityContext.h>   // 需确认构造函数存在
#include <mc/deps/vanilla_components/ItemActorFlagComponent.h>
#include <mc/entity/components/ActorTickNeededComponent.h>
#include <mc/entity/systems/ActorLegacyTickSystem.h>
#include <mc/world/actor/Actor.h>
#include <mc/world/actor/item/ItemActor.h>
#include <mc/world/level/BlockSource.h>

#include <entt/entt.hpp>

namespace parallel_item_tick {

// ============================================================
// 全局变量定义
// ============================================================
Config                    gConfig;
std::shared_ptr<ll::io::Logger> gLogger;
bool                       gStatsRunning = false;
std::unique_ptr<TickWorkerPool> gWorkerPool;

thread_local bool gSuppressMerge = false;

bool gSkipProcessedItems = false;

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
// 配置加载/保存（自动生成默认配置）
// ============================================================
bool loadConfig() {
    auto path = ll::mod::NativeMod::current()->getConfigDir() / "config.json";

    // 检查配置文件是否存在
    if (!std::filesystem::exists(path)) {
        // 文件不存在，直接保存默认配置
        getLogger().info("Configuration file not found, creating default config.json");
        return saveConfig(); // saveConfig 会创建文件并写入默认值
    }

    // 文件存在，尝试加载
    auto res = ll::config::loadConfig(gConfig, path);
    if (!res) {
        getLogger().warn("Failed to load config, using defaults (config file will not be overwritten)");
    }

    // 验证/修正配置
    if (gConfig.minParallelCnt < 1)  gConfig.minParallelCnt = 16;
    if (gConfig.workerThreads < 0)   gConfig.workerThreads = 0;
    if (gConfig.workerThreads == 0) {
        int cores = (int)std::thread::hardware_concurrency();
        gConfig.workerThreads = std::max(1, cores - 1);
        gConfig.workerThreads = std::min(gConfig.workerThreads, 7); // 限制
    }

    return res;
}

bool saveConfig() {
    auto path = ll::mod::NativeMod::current()->getConfigDir() / "config.json";
    return ll::config::saveConfig(gConfig, path);
}

// ============================================================
// 统计任务 (每 5 秒运行)
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
                double avgProc = totalTicks ? (double)totalProcess / totalTicks : 0.0;

                getLogger().info(
                    "Stats (5s): frames={}, items/frame={:.1f}, merges/frame={:.1f}, "
                    "avgTime={:.2f}ms, maxTime={:.2f}ms",
                    totalTicks, avgProc, totalTicks ? (double)totalMerged / totalTicks : 0.0,
                    avgTime / 1000.0, maxTime / 1000.0
                );

                // 重置统计（显示最近5s数据）
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

void stopStatsTask() {
    gStatsRunning = false;
}

// ============================================================
// TickWorkerPool 成员函数实现
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

void TickWorkerPool::workerMain(int /*id*/) {
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
// Hook 1: ItemActor::_mergeWithNeighbours (跳过并行阶段)
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
    if (!gSuppressMerge) {
        gTotalMerged.fetch_add(1, std::memory_order_relaxed);
    }
}

// ============================================================
// Hook 2: ActorLegacyTickSystem::tickActorLegacyTickSystem
//         跳过已被并行处理的 ItemActor
// ============================================================
LL_STATIC_HOOK(
    PerEntityTickHook,
    ll::memory::HookPriority::Normal,
    ActorLegacyTickSystem::tickActorLegacyTickSystem,
    void,
    ::EntityContext& entity,
    ::Actor&         actor,
    ::ActorTickNeededComponent& tickComp
) {
    if (gSkipProcessedItems && entity.hasComponent<ItemActorFlagComponent>()) {
        return;
    }
    origin(entity, actor, tickComp);
}

// ============================================================
// Hook 3: ActorLegacyTickSystem::tick（修正虚函数地址）
// ============================================================
LL_TYPE_INSTANCE_HOOK(
    SystemTickHook,
    ll::memory::HookPriority::Normal,
    ActorLegacyTickSystem,
    &ActorLegacyTickSystem::$tick,   // 注意 $ 前缀
    void,
    ::EntityRegistry& registry
) {
    if (!gConfig.enabled) {
        origin(registry);
        return;
    }

    auto tickStart = std::chrono::steady_clock::now();

    struct ItemEntry {
        ItemActor* actor;
        BlockSource* region;
    };
    std::vector<ItemEntry> items;
    items.reserve(256);

    auto view = registry.view<ItemActorFlagComponent, ActorTickNeededComponent>();
    for (auto entity : view) {
        auto& tickComp = view.get<ActorTickNeededComponent>(entity);
        // 构造 EntityContext，假设存在构造函数 EntityContext(EntityRegistry&, EntityId)
        // 如果编译错误，请参考实际 EntityContext.h 提供的构造方式
        EntityContext ctx(registry, entity);
        Actor* actor = Actor::tryGetFromEntity(ctx, false);
        if (!actor) continue;
        auto regionRef = tickComp.mBlockSource.lock();
        if (!regionRef) continue;
        items.emplace_back(static_cast<ItemActor*>(actor), regionRef.get());
    }

    size_t count = items.size();

    if (count < (size_t)gConfig.minParallelCnt || count == 0) {
        origin(registry);
        if (gConfig.debug) {
            getLogger().info("Skipped parallel (count={} < min={})", count, gConfig.minParallelCnt);
        }
        return;
    }

    // 阶段 1: 并行物理 Tick
    gWorkerPool->parallelFor(count, [&items](size_t i) {
        gSuppressMerge = true;
        items[i].actor->tick(*items[i].region);
        gSuppressMerge = false;
    });

    // 阶段 2: 串行合并
    for (auto& entry : items) {
        if (entry.actor->isDead() || entry.actor->mRemoved) continue;
        entry.actor->_mergeWithNeighbours();
    }

    // 阶段 3: 让原版跳过已处理的
    gSkipProcessedItems = true;
    origin(registry);
    gSkipProcessedItems = false;

    auto elapsedUs = std::chrono::duration_cast<std::chrono::microseconds>(
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

ll::mod::NativeMod& ParallelItemTickMod::getSelf() const {
    return mSelf;
}

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
    PerEntityTickHook::hook();
    SystemTickHook::hook();

    if (gConfig.stats) {
        startStatsTask();
    }

    getLogger().info("Enabled with {} workers, minParallelCnt={}",
                     gConfig.workerThreads, gConfig.minParallelCnt);
    return true;
}

bool ParallelItemTickMod::disable() {
    stopStatsTask();

    MergeWithNeighboursHook::unhook();
    PerEntityTickHook::unhook();
    SystemTickHook::unhook();

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
