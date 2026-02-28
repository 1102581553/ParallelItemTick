#include "ParallelItemTick.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <filesystem>
#include <functional>
#include <mutex>
#include <queue>
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
#include <mc/world/item/ItemStack.h>
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
// 空间哈希
// ============================================================
struct CellKey {
    int x, y, z;
    bool operator==(const CellKey& o) const { return x == o.x && y == o.y && z == o.z; }
};

struct CellKeyHash {
    size_t operator()(const CellKey& k) const {
        size_t h = std::hash<int>()(k.x);
        h ^= std::hash<int>()(k.y) + 0x9e3779b9 + (h << 6) + (h >> 2);
        h ^= std::hash<int>()(k.z) + 0x9e3779b9 + (h << 6) + (h >> 2);
        return h;
    }
};

static CellKey posToCell(float x, float y, float z) {
    return {
        static_cast<int>(std::floor(x)),
        static_cast<int>(std::floor(y)),
        static_cast<int>(std::floor(z))
    };
}

static int getCellColor(const CellKey& key) {
    return ((key.x % 2 + 2) % 2 + (key.y % 2 + 2) % 2 + (key.z % 2 + 2) % 2) % 2;
}

// ============================================================
// 全局状态
// ============================================================
Config                          gConfig;
std::shared_ptr<ll::io::Logger> gLogger;
bool                            gStatsRunning = false;

std::atomic<uint64_t> gTotalTicks{0};
std::atomic<uint64_t> gTotalProcessed{0};
std::atomic<uint64_t> gTotalMerges{0};
std::atomic<uint64_t> gTotalTimeUs{0};
std::atomic<uint64_t> gMaxTimeUs{0};

static std::atomic<bool> gInParallelPhase{false};

// 使用 ActorUniqueID 而不是指针来跟踪已处理的实体
// 这样即使实体被销毁，ID 检查也不会访问悬空指针
static std::mutex                              gTickedMutex;
static std::unordered_set<int64_t>             gTickedIds;

static void markTicked(int64_t id) {
    std::lock_guard lock(gTickedMutex);
    gTickedIds.insert(id);
}

static bool wasTicked(int64_t id) {
    std::lock_guard lock(gTickedMutex);
    return gTickedIds.count(id) > 0;
}

static void clearTicked() {
    std::lock_guard lock(gTickedMutex);
    gTickedIds.clear();
}

// 延迟操作
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
                auto totalMrg   = gTotalMerges.load(std::memory_order_relaxed);
                auto totalTime  = gTotalTimeUs.load(std::memory_order_relaxed);
                auto maxTime    = gMaxTimeUs.load(std::memory_order_relaxed);

                getLogger().info(
                    "Stats (5s): frames={}, items/frame={:.1f}, merges/frame={:.1f}, "
                    "avgTime={:.2f}ms, maxTime={:.2f}ms, threads={}",
                    totalTicks,
                    totalTicks ? static_cast<double>(totalProc) / totalTicks : 0.0,
                    totalTicks ? static_cast<double>(totalMrg) / totalTicks : 0.0,
                    totalTicks ? static_cast<double>(totalTime) / totalTicks / 1000.0 : 0.0,
                    static_cast<double>(maxTime) / 1000.0,
                    gThreadPool ? gThreadPool->threadCount() : 0
                );

                gTotalTicks.store(0, std::memory_order_relaxed);
                gTotalProcessed.store(0, std::memory_order_relaxed);
                gTotalMerges.store(0, std::memory_order_relaxed);
                gTotalTimeUs.store(0, std::memory_order_relaxed);
                gMaxTimeUs.store(0, std::memory_order_relaxed);
            });
        }
        gStatsRunning = false;
    }).launch(ll::thread::ServerThreadExecutor::getDefault());
}

void stopStatsTask() { gStatsRunning = false; }

// ============================================================
// Hook: ItemActor::_mergeWithNeighbours — 跳过原版合并
// ============================================================
LL_TYPE_INSTANCE_HOOK(
    MergeWithNeighboursHook,
    ll::memory::HookPriority::Normal,
    ItemActor,
    &ItemActor::_mergeWithNeighbours,
    void
) {
    if (gConfig.enabled) {
        return;
    }
    origin();
}

// ============================================================
// Hook: Actor::$normalTick — 用 UniqueID 跳过已处理的
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

    // 并行阶段放行
    if (gInParallelPhase.load(std::memory_order_acquire)) {
        origin();
        return;
    }

    // 原版阶段：用 UniqueID 检查，不访问可能已销毁的对象内部
    // getOrCreateUniqueID 在 Actor 基类中，只读取 mLegacyUniqueID 成员
    // 如果实体已被 remove 但内存还在（同一帧内），这个读取是安全的
    if (this->mRemoved) {
        origin();
        return;
    }

    auto uid = this->getOrCreateUniqueID();
    if (wasTicked(uid.rawID)) {
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
// 空间哈希合并
// ============================================================
struct ItemInfo {
    ItemActor*   actor;
    BlockSource* region;
    int64_t      uid;
    float        x, y, z;
};

static size_t doSpatialMerge(std::vector<ItemInfo>& items) {
    std::unordered_map<CellKey, std::vector<size_t>, CellKeyHash> grid;
    grid.reserve(items.size());

    for (size_t i = 0; i < items.size(); ++i) {
        if (items[i].actor->mRemoved) continue;
        auto key = posToCell(items[i].x, items[i].y, items[i].z);
        grid[key].push_back(i);
    }

    size_t mergeCount = 0;

    for (size_t i = 0; i < items.size(); ++i) {
        auto* a = items[i].actor;
        if (a->mRemoved || a->isDead()) continue;

        auto& stackA = a->item();
        if (stackA.isNull()) continue;

        int maxStack = static_cast<int>(stackA.getMaxStackSize());
        if (static_cast<int>(stackA.mCount) >= maxStack) continue;

        auto cellA = posToCell(items[i].x, items[i].y, items[i].z);

        for (int dx = -1; dx <= 1; ++dx) {
            for (int dy = -1; dy <= 1; ++dy) {
                for (int dz = -1; dz <= 1; ++dz) {
                    CellKey neighborKey{cellA.x + dx, cellA.y + dy, cellA.z + dz};
                    auto it = grid.find(neighborKey);
                    if (it == grid.end()) continue;

                    for (size_t j : it->second) {
                        if (j <= i) continue;
                        auto* b = items[j].actor;
                        if (b->mRemoved || b->isDead()) continue;

                        if (a->_merge(b)) {
                            mergeCount++;
                            if (static_cast<int>(stackA.mCount) >= maxStack) goto nextItem;
                        }
                    }
                }
            }
        }
        nextItem:;
    }

    return mergeCount;
}

// ============================================================
// 并行合并：棋盘格分区
// ============================================================
static size_t doParallelSpatialMerge(std::vector<ItemInfo>& items) {
    // 构建全局空间哈希
    std::unordered_map<CellKey, std::vector<size_t>, CellKeyHash> grid;
    grid.reserve(items.size());

    for (size_t i = 0; i < items.size(); ++i) {
        if (items[i].actor->mRemoved) continue;
        auto key = posToCell(items[i].x, items[i].y, items[i].z);
        grid[key].push_back(i);
    }

    // 按 cell 颜色分组
    std::vector<CellKey> color0Cells;
    std::vector<CellKey> color1Cells;
    for (auto& [key, _] : grid) {
        if (getCellColor(key) == 0)
            color0Cells.push_back(key);
        else
            color1Cells.push_back(key);
    }

    std::atomic<size_t> totalMerges{0};

    auto processCells = [&](std::vector<CellKey>& cells) {
        if (cells.empty()) return;

        std::vector<std::function<void()>> tasks;
        // 每个任务处理一批 cell
        size_t cellsPerTask = std::max<size_t>(1, cells.size() / (gThreadPool->threadCount() * 2));
        for (size_t start = 0; start < cells.size(); start += cellsPerTask) {
            size_t end = std::min(start + cellsPerTask, cells.size());
            tasks.push_back([&, start, end] {
                size_t localMerges = 0;
                for (size_t ci = start; ci < end; ++ci) {
                    auto& cellKey = cells[ci];
                    auto  cellIt  = grid.find(cellKey);
                    if (cellIt == grid.end()) continue;

                    for (size_t idx : cellIt->second) {
                        auto* a = items[idx].actor;
                        if (a->mRemoved || a->isDead()) continue;

                        auto& stackA = a->item();
                        if (stackA.isNull()) continue;

                        int maxStack = static_cast<int>(stackA.getMaxStackSize());
                        if (static_cast<int>(stackA.mCount) >= maxStack) continue;

                        // 搜索同 cell 和相邻 cell（同色 cell 的邻居都是异色的，不会并行修改）
                        for (int dx = -1; dx <= 1; ++dx) {
                            for (int dy = -1; dy <= 1; ++dy) {
                                for (int dz = -1; dz <= 1; ++dz) {
                                    CellKey nk{cellKey.x + dx, cellKey.y + dy, cellKey.z + dz};
                                    auto nit = grid.find(nk);
                                    if (nit == grid.end()) continue;

                                    for (size_t j : nit->second) {
                                        if (j <= idx) continue;
                                        auto* b = items[j].actor;
                                        if (b->mRemoved || b->isDead()) continue;

                                        if (a->_merge(b)) {
                                            localMerges++;
                                            if (static_cast<int>(stackA.mCount) >= maxStack)
                                                goto nextItemInCell;
                                        }
                                    }
                                }
                            }
                        }
                        nextItemInCell:;
                    }
                }
                totalMerges.fetch_add(localMerges, std::memory_order_relaxed);
            });
        }

        if (tasks.size() > 1) {
            gThreadPool->submitAndWait(tasks);
        } else if (!tasks.empty()) {
            tasks[0]();
        }
    };

    // 两轮：先处理颜色0，再处理颜色1
    processCells(color0Cells);
    processCells(color1Cells);

    return totalMerges.load(std::memory_order_relaxed);
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

    std::vector<ItemInfo> items;
    items.reserve(512);

    auto actorList = getRuntimeActorList();
    for (Actor* actor : actorList) {
        if (!actor || actor->mRemoved) continue;
        if (actor->getEntityTypeId() != ActorType::ItemEntity) continue;

        auto*        itemActor = static_cast<ItemActor*>(actor);
        BlockSource& bs        = actor->getDimensionBlockSource();
        auto&        pos       = actor->getPosition();
        auto         uid       = actor->getOrCreateUniqueID();
        items.push_back({itemActor, &bs, uid.rawID, pos.x, pos.y, pos.z});
    }

    size_t count      = items.size();
    size_t mergeCount = 0;

    if (count > 0) {
        gInParallelPhase.store(true, std::memory_order_release);

        // 阶段1：并行 tick（棋盘格分区，按 chunk）
        // 按 chunk 分组
        struct ChunkKey {
            int x, z;
            bool operator==(const ChunkKey& o) const { return x == o.x && z == o.z; }
        };
        struct ChunkKeyHash {
            size_t operator()(const ChunkKey& k) const {
                return std::hash<int64_t>()(static_cast<int64_t>(k.x) << 32 |
                                            static_cast<uint32_t>(k.z));
            }
        };

        std::unordered_map<ChunkKey, std::vector<size_t>, ChunkKeyHash> chunkMap;
        for (size_t i = 0; i < count; ++i) {
            int cx = static_cast<int>(std::floor(items[i].x)) >> 4;
            int cz = static_cast<int>(std::floor(items[i].z)) >> 4;
            chunkMap[{cx, cz}].push_back(i);
        }

        // 棋盘格分两色
        std::vector<std::vector<size_t>*> color0;
        std::vector<std::vector<size_t>*> color1;
        for (auto& [key, indices] : chunkMap) {
            int color = ((key.x % 2 + 2) % 2 + (key.z % 2 + 2) % 2) % 2;
            if (color == 0)
                color0.push_back(&indices);
            else
                color1.push_back(&indices);
        }

        auto tickChunks = [&](std::vector<std::vector<size_t>*>& chunks) {
            if (chunks.empty()) return;

            std::vector<std::function<void()>> tasks;
            tasks.reserve(chunks.size());

            for (auto* chunkIndices : chunks) {
                tasks.push_back([&items, chunkIndices] {
                    for (size_t idx : *chunkIndices) {
                        auto& e = items[idx];
                        if (e.actor->mRemoved || e.actor->isDead()) continue;
                        e.actor->tick(*e.region);
                        markTicked(e.uid);
                    }
                });
            }

            if (tasks.size() > 1) {
                gThreadPool->submitAndWait(tasks);
            } else {
                tasks[0]();
            }
        };

        tickChunks(color0);
        executeDeferredActions();
        tickChunks(color1);
        executeDeferredActions();

        // 更新位置
        for (auto& e : items) {
            if (e.actor->mRemoved) continue;
            auto& pos = e.actor->getPosition();
            e.x = pos.x;
            e.y = pos.y;
            e.z = pos.z;
        }

        // 阶段2：并行空间哈希合并
        mergeCount = doParallelSpatialMerge(items);

        gInParallelPhase.store(false, std::memory_order_release);
        executeDeferredActions();
    }

    // 原版 tick，已处理的 ItemActor 通过 UniqueID 跳过
    origin();

    auto elapsedUs = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - tickStart
        ).count()
    );

    gTotalTicks.fetch_add(1, std::memory_order_relaxed);
    gTotalProcessed.fetch_add(count, std::memory_order_relaxed);
    gTotalMerges.fetch_add(mergeCount, std::memory_order_relaxed);
    gTotalTimeUs.fetch_add(elapsedUs, std::memory_order_relaxed);

    uint64_t curMax = gMaxTimeUs.load(std::memory_order_relaxed);
    while (elapsedUs > curMax &&
           !gMaxTimeUs.compare_exchange_weak(curMax, elapsedUs, std::memory_order_relaxed))
        ;

    if (gConfig.debug) {
        getLogger().info(
            "Tick: {} items, {} merges, {:.2f}ms",
            count, mergeCount, elapsedUs / 1000.0
        );
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
    MergeWithNeighboursHook::hook();
    ActorRemoveHook::hook();
    ActorKillHook::hook();
    ActorDieHook::hook();

    if (gConfig.stats) startStatsTask();

    getLogger().info(
        "Enabled — {} threads, parallel tick + parallel merge",
        threads
    );
    return true;
}

bool ParallelItemTickMod::disable() {
    stopStatsTask();
    ActorDieHook::unhook();
    ActorKillHook::unhook();
    ActorRemoveHook::unhook();
    MergeWithNeighboursHook::unhook();
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
