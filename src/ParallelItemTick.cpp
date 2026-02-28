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

static std::atomic<bool> gInCustomPhase{false};

static std::mutex                  gTickedMutex;
static std::unordered_set<int64_t> gTickedIds;

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
// Hook: ItemActor::_mergeWithNeighbours — 跳过原版 O(n²) 合并
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

    if (gInCustomPhase.load(std::memory_order_acquire)) {
        origin();
        return;
    }

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
// 合并候选对
// ============================================================
struct MergePair {
    size_t a;
    size_t b;
};

struct ItemInfo {
    ItemActor* actor;
    int64_t    uid;
    float      x, y, z;
};

struct ItemMergeInfo {
    bool  valid;
    short itemId;
    short auxValue;
    uchar count;
    uchar maxStack;
};

// 并行查找合并候选对（纯只读，不调用任何 BDS API）
static std::vector<MergePair> findMergeCandidatesParallel(
    std::vector<ItemInfo>&      items,
    std::vector<ItemMergeInfo>& mergeInfo
) {
    if (items.empty()) return {};

    // 构建空间哈希
    std::unordered_map<CellKey, std::vector<size_t>, CellKeyHash> grid;
    grid.reserve(items.size());

    for (size_t i = 0; i < items.size(); ++i) {
        if (!mergeInfo[i].valid) continue;
        auto key = posToCell(items[i].x, items[i].y, items[i].z);
        grid[key].push_back(i);
    }

    // 收集所有 cell key
    std::vector<CellKey> allCells;
    allCells.reserve(grid.size());
    for (auto& [key, _] : grid) {
        allCells.push_back(key);
    }

    if (allCells.empty()) return {};

    // 并行查找候选对
    std::mutex             pairsMutex;
    std::vector<MergePair> allPairs;

    std::vector<std::function<void()>> tasks;
    size_t cellsPerTask = std::max<size_t>(1, allCells.size() / (gThreadPool->threadCount() * 2));

    for (size_t start = 0; start < allCells.size(); start += cellsPerTask) {
        size_t end = std::min(start + cellsPerTask, allCells.size());
        tasks.push_back([&, start, end] {
            std::vector<MergePair> localPairs;

            for (size_t ci = start; ci < end; ++ci) {
                auto& cellKey = allCells[ci];
                auto  cellIt  = grid.find(cellKey);
                if (cellIt == grid.end()) continue;

                for (size_t idx : cellIt->second) {
                    auto& infoA = mergeInfo[idx];
                    if (!infoA.valid) continue;
                    if (infoA.count >= infoA.maxStack) continue;

                    for (int dx = 0; dx <= 1; ++dx) {
                        for (int dy = -1; dy <= 1; ++dy) {
                            for (int dz = (dx == 0 ? 0 : -1); dz <= 1; ++dz) {
                                CellKey nk{cellKey.x + dx, cellKey.y + dy, cellKey.z + dz};
                                auto    nit = grid.find(nk);
                                if (nit == grid.end()) continue;

                                for (size_t j : nit->second) {
                                    if (j <= idx) continue;
                                    auto& infoB = mergeInfo[j];
                                    if (!infoB.valid) continue;

                                    if (infoA.itemId != infoB.itemId) continue;
                                    if (infoA.auxValue != infoB.auxValue) continue;

                                    float ddx = items[idx].x - items[j].x;
                                    float ddy = items[idx].y - items[j].y;
                                    float ddz = items[idx].z - items[j].z;
                                    if (ddx * ddx + ddy * ddy + ddz * ddz > 1.0f) continue;

                                    localPairs.push_back({idx, j});
                                }
                            }
                        }
                    }
                }
            }

            if (!localPairs.empty()) {
                std::lock_guard lock(pairsMutex);
                allPairs.insert(allPairs.end(), localPairs.begin(), localPairs.end());
            }
        });
    }

    if (tasks.size() > 1) {
        gThreadPool->submitAndWait(tasks);
    } else if (!tasks.empty()) {
        tasks[0]();
    }

    return allPairs;
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

        auto* itemActor = static_cast<ItemActor*>(actor);
        auto  uid       = actor->getOrCreateUniqueID();
        auto& pos       = actor->getPosition();
        items.push_back({itemActor, uid.rawID, pos.x, pos.y, pos.z});
    }

    size_t count      = items.size();
    size_t mergeCount = 0;
    size_t candidates = 0;

    if (count > 0) {
        gInCustomPhase.store(true, std::memory_order_release);

        // 阶段1：主线程串行 tick（_mergeWithNeighbours 被跳过）
        for (auto& e : items) {
            if (e.actor->mRemoved || e.actor->isDead()) continue;
            BlockSource& bs = e.actor->getDimensionBlockSource();
            e.actor->tick(bs);
            markTicked(e.uid);
        }

        // 更新位置 + 预计算合并信息（主线程读取 BDS 数据）
        std::vector<ItemMergeInfo> mergeInfo(count);
        for (size_t i = 0; i < count; ++i) {
            auto* a = items[i].actor;
            if (a->mRemoved || a->isDead()) {
                mergeInfo[i].valid = false;
                continue;
            }
            auto& pos = a->getPosition();
            items[i].x = pos.x;
            items[i].y = pos.y;
            items[i].z = pos.z;

            auto& stack = a->item();
            if (stack.isNull()) {
                mergeInfo[i].valid = false;
                continue;
            }
            mergeInfo[i].valid    = true;
            mergeInfo[i].itemId   = stack.getId();
            mergeInfo[i].auxValue = stack.getAuxValue();
            mergeInfo[i].count    = stack.mCount;
            mergeInfo[i].maxStack = stack.getMaxStackSize();
        }

        // 阶段2：并行查找合并候选对（纯只读计算）
        auto pairs = findMergeCandidatesParallel(items, mergeInfo);
        candidates = pairs.size();

        // 阶段3：主线程串行执行合并
        for (auto& p : pairs) {
            auto* a = items[p.a].actor;
            auto* b = items[p.b].actor;
            if (a->mRemoved || a->isDead()) continue;
            if (b->mRemoved || b->isDead()) continue;

            if (a->_merge(b)) {
                mergeCount++;
            }
        }

        gInCustomPhase.store(false, std::memory_order_release);
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
            "Tick: {} items, {} merges, {} candidates, {:.2f}ms",
            count, mergeCount, candidates, elapsedUs / 1000.0
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

    if (gConfig.stats) startStatsTask();

    getLogger().info(
        "Enabled — {} threads, parallel candidate search + O(n) spatial hash",
        threads
    );
    return true;
}

bool ParallelItemTickMod::disable() {
    stopStatsTask();
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
