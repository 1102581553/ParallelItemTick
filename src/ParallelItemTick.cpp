// ParallelItemTick.cpp
#include "ParallelItemTick.h"

#include <chrono>
#include <filesystem>
#include <latch>
#include <vector>

#include <ll/api/Config.h>
#include <ll/api/coro/CoroTask.h>
#include <ll/api/io/Logger.h>
#include <ll/api/io/LoggerRegistry.h>
#include <ll/api/memory/Hook.h>
#include <ll/api/mod/NativeMod.h>
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
Config                         gConfig;
std::shared_ptr<ll::io::Logger> gLogger;
bool                           gStatsRunning    = false;
std::unique_ptr<TickWorkerPool> gWorkerPool;

thread_local bool gSuppressMerge     = false;
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
    auto path = ll::mod::NativeMod::current()->getConfigDir() / "config.json";
    if (!std::filesystem::exists(path)) {
        getLogger().info("Config not found, creating default config.json");
        return saveConfig();
    }
    auto res = ll::config::loadConfig(gConfig, path);
    if (!res) {
        getLogger().warn("Failed to load config, using defaults");
    }
    if (gConfig.minParallelCnt < 1) gConfig.minParallelCnt = 16;
    if (gConfig.workerThreads  < 0) gConfig.workerThreads  = 0;
    if (gConfig.workerThreads == 0) {
        int cores             = (int)std::thread::hardware_concurrency();
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

    executeWork(
