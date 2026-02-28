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
#include <unordered_map>
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

    // 提交 N 个独立任务，等待全部完成
    void submitAndWait(std::vector<std::function<void()>>& tasks) {
        if (tasks.empty()) return;

        std::atomic<size_t>     completed{0};
        size_t                  total = tasks.size();
        std::mutex              doneMutex;
        std::condition_variable doneCv;

        for (auto& task : tasks) {
            {
                std::unique_lock lock(mMutex);
                mTasks.push([&task, &completed, &doneCv] {
                    task();
                    completed.fetch_add(1, std::memory_order_release);
                    doneCv.notify_one();
                });
            }
            mCondition.notify_one();
        }

        std::unique_lock lock(doneMutex);
        doneCv.wait(lock, [&] {
            return completed.load(std::memory_order_acquire) >= total;
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
// 并行阶段标志
// ============================================================
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
// 空间分区 — 棋盘格模式
// 将世界按 chunk（16x16）分区，用棋盘格着色
// 同一颜色的 chunk 之间至少隔一个 chunk，不会互相影响
// 分两轮执行：先黑格并行，再白格并行
// ============================================================
struct ChunkKey {
    int x, z;
    bool operator==(const ChunkKey& o) const { return x == o.x && z == o.z; }
};

struct ChunkKeyHash {
    size_t operator()(const ChunkKey& k) const {
        return std::hash<int64_t>()(static_cast<int64_t>(k.x) << 32 | static_cast<uint32_t>(k.z));
    }
};

struct ItemEntry {
    ItemActor*   actor;
    BlockSource* region;
};

static ChunkKey getChunkKey(const Actor* actor) {
    auto& pos = actor->getPosition();
    return {
        static_cast<int>(std::floor(pos.x)) >> 4,
        static_cast<int>(std::floor(pos.z)) >> 4
    };
}

// 棋盘格颜色：(chunkX + chunkZ) % 2
static int getChunkColor(const ChunkKey& key) {
    return ((key.x % 2 + 2) % 2 + (key.z % 2 + 2) % 2) % 2;
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
// Hook: Actor::$normalTick
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

    // 并行阶段由工作线程调用，放行
    if (gInParallelPhase.load(std::memory_order_acquire)) {
        origin();
        return;
    }

    // 原版阶段，跳过已处理的
    if (wasTicked(this)) {
        return;
    }

    origin();
}

// ============================================================
// Hook: Actor::$remove — 并行阶段延迟
// ============================================================
LL_TYPE_INSTANCE_HOOK(
    ActorRemoveHook,
    ll::memory::HookPriority::Normal,
    Actor,
    &Actor::$remove,
    void
) {
    if (gInParallelPhase.load(std::memory_order_acquire)) {
        this->mRemoved = true;
        Actor* self = this;
        deferAction([self] { self->$remove(); });
        return;
    }
    origin();
}

// ============================================================
// Hook: Actor::$kill — 并行阶段延迟
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
        deferAction([self] { self->$kill(); });
        return;
    }
    origin();
}

// ============================================================
// Hook: Actor::$die — 并行阶段延迟
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
        Actor* self = this;
        deferAction([self] { self->$kill(); });
        return;
    }
    origin(source);
}

// ============================================================
// Hook: Level::$tick — 棋盘格并行
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

    // 收集所有 ItemActor，按 chunk 分组
    std::unordered_map<ChunkKey, std::vector<ItemEntry>, ChunkKeyHash> chunkMap;

    auto actorList = getRuntimeActorList();
    size_t totalCount = 0;

    for (Actor* actor : actorList) {
        if (!actor || actor->mRemoved) continue;
        if (actor->getEntityTypeId() != ActorType::ItemEntity) continue;

        auto*        itemActor = static_cast<ItemActor*>(actor);
        BlockSource& bs        = actor->getDimensionBlockSource();
        auto         key       = getChunkKey(actor);

        chunkMap[key].push_back({itemActor, &bs});
        totalCount++;
    }

    if (totalCount > 0) {
        // 按棋盘格颜色分成两组 chunk
        std::vector<std::vector<ItemEntry>*> color0Chunks;
        std::vector<std::vector<ItemEntry>*> color1Chunks;

        for (auto& [key, entries] : chunkMap) {
            if (getChunkColor(key) == 0) {
                color0Chunks.push_back(&entries);
            } else {
                color1Chunks.push_back(&entries);
            }
        }

        // 处理一组 chunk 的 lambda
        auto processChunks = [this](std::vector<std::vector<ItemEntry>*>& chunks) {
            if (chunks.empty()) return;

            gInParallelPhase.store(true, std::memory_order_release);

            // 每个 chunk 作为一个独立任务
            std::vector<std::function<void()>> tasks;
            tasks.reserve(chunks.size());

            for (auto* chunkEntries : chunks) {
                tasks.push_back([chunkEntries] {
                    for (auto& e : *chunkEntries) {
                        if (e.actor->mRemoved) continue;
                        if (e.actor->isDead()) continue;

                        e.actor->tick(*e.region);
                        markTicked(e.actor);
                    }
                });
            }

            gThreadPool->submitAndWait(tasks);

            gInParallelPhase.store(false, std::memory_order_release);

            // 执行本轮延迟操作
            executeDeferredActions();
        };

        // 第一轮：黑格
        processChunks(color0Chunks);
        // 第二轮：白格
        processChunks(color1Chunks);
    }

    // 原版 tick，已处理的 ItemActor 会被跳过
    origin();

    auto elapsedUs = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - tickStart
        ).count()
    );

    gTotalTicks.fetch_add(1, std::memory_order_relaxed);
    gTotalProcessed.fetch_add(totalCount, std::memory_order_relaxed);
    gTotalTimeUs.fetch_add(elapsedUs, std::memory_order_relaxed);

    uint64_t curMax = gMaxTimeUs.load(std::memory_order_relaxed);
    while (elapsedUs > curMax &&
           !gMaxTimeUs.compare_exchange_weak(curMax, elapsedUs, std::memory_order_relaxed))
        ;

    if (gConfig.debug) {
        getLogger().info("Tick: {} items parallel, {:.2f}ms", totalCount, elapsedUs / 1000.0);
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

    getLogger().info("Enabled — {} worker threads, checkerboard partitioning", threads);
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
