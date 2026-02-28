// ParallelItemTick.cpp
#include "ParallelItemTick.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <unordered_map>
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

bool gSkipProcessedItems = false;

std::atomic<uint64_t> gTotalTicks{0};
std::atomic<uint64_t> gTotalProcessed{0};
std::atomic<uint64_t> gTotalMerged{0};
std::atomic<uint64_t> gTotalTimeUs{0};
std::atomic<uint64_t> gMaxTimeUs{0};

// ============================================================
// 工具函数
// ============================================================
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
    if (gConfig.minParallelCnt < 1) gConfig.minParallelCnt = 16;
    if (gConfig.workerThreads < 0)  gConfig.workerThreads  = 0;
    if (gConfig.workerThreads == 0) {
        int cores             = static_cast<int>(std::thread::hardware_concurrency());
        gConfig.workerThreads = std::max(1, std::min(cores - 1, 7));
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

                auto totalTicks   = gTotalTicks.load(std::memory_order_relaxed);
                auto totalProcess = gTotalProcessed.load(std::memory_order_relaxed);
                auto totalMerged  = gTotalMerged.load(std::memory_order_relaxed);
                auto totalTime    = gTotalTimeUs.load(std::memory_order_relaxed);
                auto maxTime      = gMaxTimeUs.load(std::memory_order_relaxed);

                getLogger().info(
                    "Stats (5s): frames={}, items/frame={:.1f}, merges/frame={:.1f}, "
                    "avgTime={:.2f}ms, maxTime={:.2f}ms",
                    totalTicks,
                    totalTicks ? static_cast<double>(totalProcess) / totalTicks : 0.0,
                    totalTicks ? static_cast<double>(totalMerged) / totalTicks : 0.0,
                    totalTicks ? static_cast<double>(totalTime) / totalTicks / 1000.0 : 0.0,
                    static_cast<double>(maxTime) / 1000.0
                );

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

void stopStatsTask() { gStatsRunning = false; }

// ============================================================
// TickWorkerPool — mutex+cv 实现，无竞态
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
    mWorkCv.notify_all();
    for (auto& w : mWorkers) {
        if (w.joinable()) w.join();
    }
}

void TickWorkerPool::parallelFor(size_t count, std::function<void(size_t)> const& func) {
    if (count == 0) return;

    // 任务太少，主线程直接串行
    if (count < static_cast<size_t>(gConfig.minParallelCnt) ||
        mNumWorkers == 0) {
        for (size_t i = 0; i < count; i++) func(i);
        return;
    }

    // 发布任务
    {
        std::lock_guard lock(mMutex);
        mWorkFunc      = &func;
        mWorkCount     = count;
        mNextIndex.store(0, std::memory_order_relaxed);
        mActiveWorkers = mNumWorkers;
        mGeneration++;
    }
    mWorkCv.notify_all();

    // 主线程也参与抢任务
    while (true) {
        size_t idx = mNextIndex.fetch_add(1, std::memory_order_acq_rel);
        if (idx >= count) break;
        func(idx);
    }

    // 等待所有 worker 完成
    {
        std::unique_lock lock(mMutex);
        mDoneCv.wait(lock, [this] { return mActiveWorkers == 0; });
    }

    mWorkFunc = nullptr;
}

void TickWorkerPool::workerMain() {
    uint64_t lastGen = 0;
    while (true) {
        // 等待新任务或关闭信号
        {
            std::unique_lock lock(mMutex);
            mWorkCv.wait(lock, [this, lastGen] {
                return mShutdown || mGeneration > lastGen;
            });
            if (mShutdown) return;
            lastGen = mGeneration;
        }

        // 抢任务执行
        while (true) {
            size_t idx = mNextIndex.fetch_add(1, std::memory_order_acq_rel);
            if (idx >= mWorkCount) break;
            try {
                (*mWorkFunc)(idx);
            } catch (...) {
                // 吞掉异常，防止 worker 线程意外退出
            }
        }

        // 通知主线程
        {
            std::lock_guard lock(mMutex);
            mActiveWorkers--;
        }
        mDoneCv.notify_one();
    }
}

// ============================================================
// Chunk 坐标哈希，用于分组
// ============================================================
struct ChunkKey {
    int x, z;
    bool operator==(ChunkKey const& o) const { return x == o.x && z == o.z; }
};

struct ChunkKeyHash {
    size_t operator()(ChunkKey const& k) const {
        // 简单但足够的哈希
        return std::hash<int64_t>()(static_cast<int64_t>(k.x) << 32 | static_cast<uint32_t>(k.z));
    }
};

// ============================================================
// 从 Actor 位置计算 chunk 坐标
// ============================================================
static ChunkKey getChunkKey(Actor* actor) {
    auto const& pos = actor->getPosition();
    return {
        static_cast<int>(std::floor(pos.x)) >> 4,
        static_cast<int>(std::floor(pos.z)) >> 4
    };
}

// ============================================================
// Hook: Actor::tick — 跳过已并行处理的 ItemActor
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
// Hook: Level::$tick — 主逻辑，棋盘格并行
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

    // ── 收集所有存活的 ItemActor ──
    struct ItemEntry {
        ItemActor*   actor;
        BlockSource* region;
        ChunkKey     chunk;
    };

    std::vector<ItemEntry> items;
    items.reserve(256);

    auto actorList = getRuntimeActorList();
    for (Actor* actor : actorList) {
        if (!actor || actor->mRemoved) continue;
        auto* itemActor = dynamic_cast<ItemActor*>(actor);
        if (!itemActor) continue;
        BlockSource& bs = actor->getDimensionBlockSource();
        items.push_back({itemActor, &bs, getChunkKey(actor)});
    }

    size_t count = items.size();

    if (count < static_cast<size_t>(gConfig.minParallelCnt)) {
        origin();
        if (gConfig.debug) {
            getLogger().info("Skipped parallel (count={} < min={})", count, gConfig.minParallelCnt);
        }
        return;
    }

    // ── 按 chunk 分组 ──
    // key = ChunkKey, value = 该 chunk 内的 item 索引列表
    std::unordered_map<ChunkKey, std::vector<size_t>, ChunkKeyHash> chunkMap;
    chunkMap.reserve(count / 4);
    for (size_t i = 0; i < count; i++) {
        chunkMap[items[i].chunk].push_back(i);
    }

    // ── 棋盘格分 4 个 phase ──
    // phase 由 (chunkX % 2, chunkZ % 2) 决定
    // 同一 phase 内的 chunk 互不相邻，可以安全并行
    // 不同 phase 串行执行
    std::vector<std::vector<size_t>> phases[4];

    for (auto& [key, indices] : chunkMap) {
        // C++ 的 % 对负数可能返回负值，用位运算确保 0 或 1
        int px = key.x & 1;
        int pz = key.z & 1;
        int phase = (px << 1) | pz;
        // 每个 chunk 的所有 item 作为一个工作单元
        phases[phase].push_back(std::move(indices));
    }

    size_t mergeCount = 0;

    // ── 分 4 个 phase 执行 ──
    for (int p = 0; p < 4; p++) {
        auto& chunks = phases[p];
        if (chunks.empty()) continue;

        // parallelFor 的粒度是 chunk（不是单个 item）
        // 同一 chunk 内的 item 串行 tick，不同 chunk 并行
        gWorkerPool->parallelFor(chunks.size(), [&](size_t chunkIdx) {
            for (size_t itemIdx : chunks[chunkIdx]) {
                auto& e = items[itemIdx];
                try {
                    e.actor->tick(*e.region);
                } catch (...) {
                    // 防止单个实体异常导致整个 phase 崩溃
                }
            }
        });
    }

    // ── 串行 merge ──
    for (auto& e : items) {
        if (e.actor->mRemoved || e.actor->isDead()) continue;
        try {
            e.actor->_mergeWithNeighbours();
            mergeCount++;
        } catch (...) {}
    }

    // ── 原版 tick，跳过已处理的 ItemActor ──
    gSkipProcessedItems = true;
    origin();
    gSkipProcessedItems = false;

    // ── 统计 ──
    auto elapsedUs = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - tickStart
        ).count()
    );

    gTotalTicks.fetch_add(1, std::memory_order_relaxed);
    gTotalProcessed.fetch_add(count, std::memory_order_relaxed);
    gTotalMerged.fetch_add(mergeCount, std::memory_order_relaxed);
    gTotalTimeUs.fetch_add(elapsedUs, std::memory_order_relaxed);

    uint64_t curMax = gMaxTimeUs.load(std::memory_order_relaxed);
    while (elapsedUs > curMax &&
           !gMaxTimeUs.compare_exchange_weak(curMax, elapsedUs, std::memory_order_relaxed))
        ;

    if (gConfig.debug) {
        getLogger().info(
            "Tick: {} items, {} chunks, {:.2f}ms",
            count, chunkMap.size(), elapsedUs / 1000.0
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

    ActorTickHook::hook();
    LevelTickHook::hook();

    if (gConfig.stats) startStatsTask();

    getLogger().info(
        "Enabled with {} workers, minParallelCnt={}, checkerboard parallel",
        gConfig.workerThreads, gConfig.minParallelCnt
    );
    return true;
}

bool ParallelItemTickMod::disable() {
    stopStatsTask();

    ActorTickHook::unhook();
    LevelTickHook::unhook();

    gWorkerPool.reset();
    gSkipProcessedItems = false;

    getLogger().info("Disabled");
    return true;
}

} // namespace parallel_item_tick

LL_REGISTER_MOD(
    parallel_item_tick::ParallelItemTickMod,
    parallel_item_tick::ParallelItemTickMod::getInstance()
);
