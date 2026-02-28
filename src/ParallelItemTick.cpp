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
// Per-dimension per-thread BlockSource 池
// ============================================================
struct DimBlockSources {
    std::vector<std::unique_ptr<BlockSource>> sources;
};
static std::unordered_map<int, DimBlockSources> gBlockSourcePool;

static void ensureBlockSources(BlockSource& originalBS, int dimId, int totalSlots) {
    auto& entry = gBlockSourcePool[dimId];
    if (entry.sources.size() >= static_cast<size_t>(totalSlots)) return;

    entry.sources.reserve(totalSlots);
    while (entry.sources.size() < static_cast<size_t>(totalSlots)) {
        entry.sources.push_back(std::make_unique<BlockSource>(
            originalBS.getLevel(),
            originalBS.getDimension(),
            originalBS.getChunkSource(),
            true,
            false,
            false
        ));
    }
}

static void clearBlockSourcePool() {
    gBlockSourcePool.clear();
}

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
// TickWorkerPool
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

    if (count < static_cast<size_t>(gConfig.minParallelCnt) || mNumWorkers == 0) {
        for (size_t i = 0; i < count; i++) func(i);
        return;
    }

    {
        std::lock_guard lock(mMutex);
        mWorkFunc      = &func;
        mWorkCount     = count;
        mNextIndex.store(0, std::memory_order_relaxed);
        mActiveWorkers = mNumWorkers;
        mGeneration++;
    }
    mWorkCv.notify_all();

    while (true) {
        size_t idx = mNextIndex.fetch_add(1, std::memory_order_acq_rel);
        if (idx >= count) break;
        func(idx);
    }

    {
        std::unique_lock lock(mMutex);
        mDoneCv.wait(lock, [this] { return mActiveWorkers == 0; });
    }

    mWorkFunc = nullptr;
}

void TickWorkerPool::workerMain() {
    uint64_t lastGen = 0;
    while (true) {
        {
            std::unique_lock lock(mMutex);
            mWorkCv.wait(lock, [this, lastGen] {
                return mShutdown || mGeneration > lastGen;
            });
            if (mShutdown) return;
            lastGen = mGeneration;
        }

        while (true) {
            size_t idx = mNextIndex.fetch_add(1, std::memory_order_acq_rel);
            if (idx >= mWorkCount) break;
            try {
                (*mWorkFunc)(idx);
            } catch (...) {}
        }

        {
            std::lock_guard lock(mMutex);
            mActiveWorkers--;
        }
        mDoneCv.notify_one();
    }
}

// ============================================================
// 线程槽位
// ============================================================
static thread_local int tSlotId = -1;
static std::atomic<int> gNextSlot{1};

static int getSlotId() {
    if (tSlotId < 0) {
        tSlotId = gNextSlot.fetch_add(1, std::memory_order_relaxed);
    }
    return tSlotId;
}

// ============================================================
// Chunk 分组
// ============================================================
struct ChunkKey {
    int dimId;
    int x, z;
    bool operator==(ChunkKey const& o) const {
        return dimId == o.dimId && x == o.x && z == o.z;
    }
};

struct ChunkKeyHash {
    size_t operator()(ChunkKey const& k) const {
        size_t h = std::hash<int>()(k.dimId);
        h ^= std::hash<int>()(k.x) + 0x9e3779b9 + (h << 6) + (h >> 2);
        h ^= std::hash<int>()(k.z) + 0x9e3779b9 + (h << 6) + (h >> 2);
        return h;
    }
};

// ============================================================
// Hook: Actor::tick
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
// Hook: Level::$tick
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
        BlockSource* originalBS;
        int          dimId;
        int          chunkX;
        int          chunkZ;
    };

    std::vector<ItemEntry> items;
    items.reserve(256);

    auto actorList = getRuntimeActorList();
    for (Actor* actor : actorList) {
        if (!actor || actor->mRemoved) continue;
        auto* itemActor = dynamic_cast<ItemActor*>(actor);
        if (!itemActor) continue;

        BlockSource& bs = actor->getDimensionBlockSource();
        int dimId = static_cast<int>(bs.getDimensionId());

        auto const& pos = actor->getPosition();
        int cx = static_cast<int>(std::floor(pos.x)) >> 4;
        int cz = static_cast<int>(std::floor(pos.z)) >> 4;

        items.push_back({itemActor, &bs, dimId, cx, cz});
    }

    size_t count = items.size();

    if (count < static_cast<size_t>(gConfig.minParallelCnt)) {
        origin();
        if (gConfig.debug) {
            getLogger().info("Skipped parallel (count={} < min={})", count, gConfig.minParallelCnt);
        }
        return;
    }

    // ── 准备 per-thread BlockSource ──
    int totalSlots = gWorkerPool->getNumWorkers() + 1;

    std::unordered_map<int, BlockSource*> involvedDims;
    for (auto& e : items) {
        involvedDims.emplace(e.dimId, e.originalBS);
    }
    for (auto& [dimId, bs] : involvedDims) {
        ensureBlockSources(*bs, dimId, totalSlots);
    }

    tSlotId = 0;
    gNextSlot.store(1, std::memory_order_relaxed);

    // ── 按 chunk 分组 ──
    std::unordered_map<ChunkKey, std::vector<size_t>, ChunkKeyHash> chunkMap;
    chunkMap.reserve(count / 4);
    for (size_t i = 0; i < count; i++) {
        ChunkKey key{items[i].dimId, items[i].chunkX, items[i].chunkZ};
        chunkMap[key].push_back(i);
    }

    // ── 棋盘格 4 phase ──
    std::vector<std::vector<size_t>> phases[4];

    for (auto& [key, indices] : chunkMap) {
        int px = key.x & 1;
        int pz = key.z & 1;
        int phase = (px << 1) | pz;
        phases[phase].push_back(std::move(indices));
    }

    // ── 分 phase 执行 ──
    for (int p = 0; p < 4; p++) {
        auto& chunks = phases[p];
        if (chunks.empty()) continue;

        gWorkerPool->parallelFor(chunks.size(), [&](size_t chunkIdx) {
            int slot = getSlotId();

            for (size_t itemIdx : chunks[chunkIdx]) {
                auto& e = items[itemIdx];

                auto& pool = gBlockSourcePool[e.dimId];
                int safeSlot = (slot < static_cast<int>(pool.sources.size())) ? slot : 0;
                BlockSource& threadBS = *pool.sources[safeSlot];

                try {
                    e.actor->tick(threadBS);
                } catch (...) {}
            }
        });

        tSlotId = 0;
        gNextSlot.store(1, std::memory_order_relaxed);
    }

    // ── 串行 merge ──
    size_t mergeCount = 0;
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
        "Enabled with {} workers, per-thread BlockSource, checkerboard parallel",
        gConfig.workerThreads
    );
    return true;
}

bool ParallelItemTickMod::disable() {
    stopStatsTask();

    ActorTickHook::unhook();
    LevelTickHook::unhook();

    gWorkerPool.reset();
    clearBlockSourcePool();
    gSkipProcessedItems = false;

    getLogger().info("Disabled");
    return true;
}

} // namespace parallel_item_tick

LL_REGISTER_MOD(
    parallel_item_tick::ParallelItemTickMod,
    parallel_item_tick::ParallelItemTickMod::getInstance()
);
