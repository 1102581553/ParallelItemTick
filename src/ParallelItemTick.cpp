#include "ParallelItemTick.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <functional>
#include <mutex>
#include <queue>
#include <shared_mutex>
#include <thread>
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
#include <mc/world/actor/ActorType.h>
#include <mc/world/actor/item/ItemActor.h>
#include <mc/world/level/BlockSource.h>
#include <mc/world/level/Level.h>

namespace parallel_item_tick {

// ============================================================
// 线程池
// ============================================================
class ThreadPool {
public:
    explicit ThreadPool(size_t threads) : mStop(false) {
        for (size_t i = 0; i < threads; ++i) {
            mWorkers.emplace_back([this] { workerLoop(); });
        }
    }

    ~ThreadPool() {
        {
            std::unique_lock lock(mMutex);
            mStop = true;
        }
        mCondition.notify_all();
        for (auto& w : mWorkers) {
            if (w.joinable()) w.join();
        }
    }

    void parallelFor(size_t count, size_t batchSize, std::function<void(size_t, size_t)> func) {
        if (count == 0) return;

        std::atomic<size_t>     completedBatches{0};
        size_t                  totalBatches = (count + batchSize - 1) / batchSize;
        std::mutex              doneMutex;
        std::condition_variable doneCv;

        for (size_t batchStart = 0; batchStart < count; batchStart += batchSize) {
            size_t batchEnd = std::min(batchStart + batchSize, count);
            {
                std::unique_lock lock(mMutex);
                mTasks.push([&func, &completedBatches, &doneCv, batchStart, batchEnd] {
                    func(batchStart, batchEnd);
                    completedBatches.fetch_add(1, std::memory_order_release);
                    doneCv.notify_one();
                });
            }
            mCondition.notify_one();
        }

        std::unique_lock lock(doneMutex);
        doneCv.wait(lock, [&] {
            return completedBatches.load(std::memory_order_acquire) >= totalBatches;
        });
    }

    size_t threadCount() const { return mWorkers.size(); }

private:
    void workerLoop() {
        while (true) {
            std::function<void()> task;
            {
                std::unique_lock lock(mMutex);
                mCondition.wait(lock, [this] { return mStop || !mTasks.empty(); });
                if (mStop && mTasks.empty()) return;
                task = std::move(mTasks.front());
                mTasks.pop();
            }
            task();
        }
    }

    std::vector<std::thread>          mWorkers;
    std::queue<std::function<void()>> mTasks;
    std::mutex                        mMutex;
    std::condition_variable           mCondition;
    bool                              mStop;
};

static std::unique_ptr<ThreadPool> gThreadPool;

// ============================================================
// 全局并行锁
// 并行阶段：主线程持有独占锁（阻止其他线程修改实体）
//           工作线程安全执行（主线程在 parallelFor 中等待）
// 实体销毁：需要获取独占锁（如果并行阶段正在进行则等待）
// ============================================================
std::shared_mutex gParallelMutex;

// 并行阶段标志
static std::atomic<bool> gInParallelPhase{false};

// ============================================================
// 延迟操作队列
// ============================================================
static std::mutex                         gDeferredMutex;
static std::vector<std::function<void()>> gDeferredActions;

static void deferAction(std::function<void()> action) {
    std::lock_guard lock(gDeferredMutex);
    gDeferredActions.push_back(std::move(action));
}

static void executeDeferredActions() {
    std::vector<std::function<void()>> actions;
    {
        std::lock_guard lock(gDeferredMutex);
        actions.swap(gDeferredActions);
    }
    for (auto& a : actions) {
        a();
    }
}

// ============================================================
// 已处理集合
// ============================================================
static std::mutex                       gTickedMutex;
static std::unordered_set<const Actor*> gTickedItems;

static void markTicked(const Actor* item) {
    std::lock_guard lock(gTickedMutex);
    gTickedItems.insert(item);
}

static bool wasTicked(const Actor* item) {
    std::lock_guard lock(gTickedMutex);
    return gTickedItems.count(item) > 0;
}

static void clearTicked() {
    std::lock_guard lock(gTickedMutex);
    gTickedItems.clear();
}

// ============================================================
// 全局状态
// ============================================================
Config                          gConfig;
std::shared_ptr<ll::io::Logger> gLogger;
bool                            gStatsRunning = false;

std::atomic<uint64_t> gTotalTicks{0};
std::atomic<uint64_t> gTotalProcessed{0};
std::atomic<uint64_t> gTotalTimeUs{0};
std::atomic<uint64_t> gMaxTimeUs{0};

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

                auto totalTicks = gTotalTicks.load(std::memory_order_relaxed);
                auto totalProc  = gTotalProcessed.load(std::memory_order_relaxed);
                auto totalTime  = gTotalTimeUs.load(std::memory_order_relaxed);
                auto maxTime    = gMaxTimeUs.load(std::memory_order_relaxed);

                getLogger().info(
                    "Stats (5s): frames={}, items/frame={:.1f}, "
                    "avgTime={:.2f}ms, maxTime={:.2f}ms, threads={}",
                    totalTicks,
                    totalTicks ? static_cast<double>(totalProc) / totalTicks : 0.0,
                    totalTicks ? static_cast<double>(totalTime) / totalTicks / 1000.0 : 0.0,
                    static_cast<double>(maxTime) / 1000.0,
                    gThreadPool ? gThreadPool->threadCount() : 0
                );

                gTotalTicks.store(0, std::memory_order_relaxed);
                gTotalProcessed.store(0, std::memory_order_relaxed);
                gTotalTimeUs.store(0, std::memory_order_relaxed);
                gMaxTimeUs.store(0, std::memory_order_relaxed);
            });
        }
        gStatsRunning = false;
    }).launch(ll::thread::ServerThreadExecutor::getDefault());
}

void stopStatsTask() { gStatsRunning = false; }

// ============================================================
// Hook: Actor::$normalTick — 拦截 ItemActor
// ============================================================
LL_TYPE_INSTANCE_HOOK(
    ActorNormalTickHook,
    ll::memory::HookPriority::Normal,
    Actor,
    &Actor::$normalTick,
    void
) {
    if (!gConfig.enabled) {
        origin();
        return;
    }

    if (this->getEntityTypeId() != ActorType::ItemEntity) {
        origin();
        return;
    }

    // 并行阶段由工作线程调用，直接执行
    if (gInParallelPhase.load(std::memory_order_acquire)) {
        origin();
        return;
    }

    // 原版阶段，跳过已并行处理的
    if (wasTicked(this)) {
        return;
    }

    origin();
}

// ============================================================
// Hook: Actor::$remove — 并行阶段延迟销毁
// ============================================================
LL_TYPE_INSTANCE_HOOK(
    ActorRemoveHook,
    ll::memory::HookPriority::Normal,
    Actor,
    &Actor::$remove,
    void
) {
    if (gInParallelPhase.load(std::memory_order_acquire)) {
        // 并行阶段：标记移除，延迟真正销毁
        this->mRemoved = true;
        Actor* self = this;
        deferAction([self] {
            self->$remove();
        });
        return;
    }
    origin();
}

// ============================================================
// Hook: Actor::kill — 并行阶段延迟 kill
// ============================================================
LL_TYPE_INSTANCE_HOOK(
    ActorKillHook,
    ll::memory::HookPriority::Normal,
    Actor,
    &Actor::$kill,
    void
) {
    if (gInParallelPhase.load(std::memory_order_acquire)) {
        this->mRemoved = true;
        Actor* self = this;
        deferAction([self] {
            self->$kill();
        });
        return;
    }
    origin();
}

// ============================================================
// Hook: Actor::die — 并行阶段延迟 die
// ============================================================
LL_TYPE_INSTANCE_HOOK(
    ActorDieHook,
    ll::memory::HookPriority::Normal,
    Actor,
    &Actor::$die,
    void,
    ::ActorDamageSource const& source
) {
    if (gInParallelPhase.load(std::memory_order_acquire)) {
        this->mRemoved = true;
        Actor*                    self = this;
        // 复制 source，因为引用可能在延迟执行时失效
        // ActorDamageSource 可能不可复制，所以只延迟 kill
        deferAction([self] {
            self->$kill();
        });
        return;
    }
    origin(source);
}

// ============================================================
// Hook: Level::$tick — 主逻辑
// ============================================================
LL_TYPE_INSTANCE_HOOK(
    LevelTickHook,
    ll::memory::HookPriority::Normal,
    Level,
    &Level::$tick,
    void
) {
    if (!gConfig.enabled || !gThreadPool) {
        origin();
        return;
    }

    auto tickStart = std::chrono::steady_clock::now();

    clearTicked();

    struct ItemEntry {
        ItemActor*   actor;
        BlockSource* region;
    };

    std::vector<ItemEntry> items;
    items.reserve(512);

    auto actorList = getRuntimeActorList();
    for (Actor* actor : actorList) {
        if (!actor || actor->mRemoved) continue;
        if (actor->getEntityTypeId() != ActorType::ItemEntity) continue;

        auto*        itemActor = static_cast<ItemActor*>(actor);
        BlockSource& bs        = actor->getDimensionBlockSource();
        items.push_back({itemActor, &bs});
    }

    size_t count = items.size();

    if (count > 0) {
        // 获取独占锁，阻止其他线程（命令处理等）修改实体
        // 注意：此时我们在主线程，命令也在主线程，所以不会死锁
        // 但异步命令或其他线程的操作会被阻塞
        gInParallelPhase.store(true, std::memory_order_release);

        gThreadPool->parallelFor(
            count,
            static_cast<size_t>(gConfig.batchSize),
            [&items](size_t begin, size_t end) {
                for (size_t i = begin; i < end; ++i) {
                    auto& e = items[i];
                    // 双重检查：可能在其他批次中被标记移除
                    if (e.actor->mRemoved) continue;
                    if (e.actor->isDead()) continue;

                    e.actor->tick(*e.region);

                    markTicked(e.actor);
                }
            }
        );

        gInParallelPhase.store(false, std::memory_order_release);

        // 执行延迟操作
        executeDeferredActions();
    }

    // 原版 tick
    origin();

    auto elapsedUs = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - tickStart
        ).count()
    );

    gTotalTicks.fetch_add(1, std::memory_order_relaxed);
    gTotalProcessed.fetch_add(count, std::memory_order_relaxed);
    gTotalTimeUs.fetch_add(elapsedUs, std::memory_order_relaxed);

    uint64_t curMax = gMaxTimeUs.load(std::memory_order_relaxed);
    while (elapsedUs > curMax &&
           !gMaxTimeUs.compare_exchange_weak(curMax, elapsedUs, std::memory_order_relaxed))
        ;

    if (gConfig.debug) {
        getLogger().info("Tick: {} items parallel, {:.2f}ms", count, elapsedUs / 1000.0);
    }
}

// ============================================================
// Mod 生命周期
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
        "Loaded. enabled={}, debug={}, stats={}, threads={}, batchSize={}",
        gConfig.enabled, gConfig.debug, gConfig.stats,
        gConfig.numThreads, gConfig.batchSize
    );
    return true;
}

bool ParallelItemTickMod::enable() {
    if (!gConfig.enabled) {
        getLogger().info("Plugin disabled by config.");
        return true;
    }

    int threads = gConfig.numThreads;
    if (threads <= 0) {
        threads = static_cast<int>(std::thread::hardware_concurrency()) - 1;
        if (threads < 2) threads = 2;
    }
    gThreadPool = std::make_unique<ThreadPool>(static_cast<size_t>(threads));

    LevelTickHook::hook();
    ActorNormalTickHook::hook();
    ActorRemoveHook::hook();
    ActorKillHook::hook();
    ActorDieHook::hook();

    if (gConfig.stats) startStatsTask();

    getLogger().info("Enabled — {} worker threads, batch size {}", threads, gConfig.batchSize);
    return true;
}

bool ParallelItemTickMod::disable() {
    stopStatsTask();
    ActorDieHook::unhook();
    ActorKillHook::unhook();
    ActorRemoveHook::unhook();
    ActorNormalTickHook::unhook();
    LevelTickHook::unhook();
    gThreadPool.reset();
    clearTicked();
    getLogger().info("Disabled");
    return true;
}

} // namespace parallel_item_tick

LL_REGISTER_MOD(
    parallel_item_tick::ParallelItemTickMod,
    parallel_item_tick::ParallelItemTickMod::getInstance()
);
