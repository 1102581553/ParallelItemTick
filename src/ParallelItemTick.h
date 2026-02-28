// ParallelItemTick.h
#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <ll/api/Config.h>
#include <ll/api/mod/NativeMod.h>

namespace ll::io {
class Logger;
}

namespace parallel_item_tick {

struct Config {
    int  version        = 1;
    bool enabled        = true;
    bool debug          = false;
    bool stats          = true;
    int  minParallelCnt = 16;
    int  workerThreads  = 0;
};

// ────────────────────────────────────────────
// 线程池：用 mutex+cv 替代 atomic::wait，
// 消除原版 generation 竞态
// ────────────────────────────────────────────
class TickWorkerPool {
public:
    explicit TickWorkerPool(int numWorkers);
    ~TickWorkerPool();

    TickWorkerPool(TickWorkerPool const&)            = delete;
    TickWorkerPool& operator=(TickWorkerPool const&) = delete;

    // 对 [0, count) 并行执行 func(i)
    void parallelFor(size_t count, std::function<void(size_t)> const& func);

private:
    void workerMain();

    int                      mNumWorkers;
    std::vector<std::thread> mWorkers;

    // 同步原语
    std::mutex              mMutex;
    std::condition_variable mWorkCv;   // 通知 worker 有新任务
    std::condition_variable mDoneCv;   // 通知主线程任务完成

    bool     mShutdown{false};
    uint64_t mGeneration{0};

    // 任务描述
    std::function<void(size_t)> const* mWorkFunc{nullptr};
    size_t                             mWorkCount{0};
    std::atomic<size_t>                mNextIndex{0};
    int                                mActiveWorkers{0};
};

// ────────────────────────────────────────────
// 全局状态
// ────────────────────────────────────────────
extern Config                          gConfig;
extern std::shared_ptr<ll::io::Logger> gLogger;
extern bool                            gStatsRunning;
extern std::unique_ptr<TickWorkerPool> gWorkerPool;

extern bool gSkipProcessedItems;

extern std::atomic<uint64_t> gTotalTicks;
extern std::atomic<uint64_t> gTotalProcessed;
extern std::atomic<uint64_t> gTotalMerged;
extern std::atomic<uint64_t> gTotalTimeUs;
extern std::atomic<uint64_t> gMaxTimeUs;

ll::io::Logger& getLogger();
bool            loadConfig();
bool            saveConfig();
void            startStatsTask();
void            stopStatsTask();

// ────────────────────────────────────────────
// 插件主类
// ────────────────────────────────────────────
class ParallelItemTickMod {
public:
    static ParallelItemTickMod& getInstance();

    ParallelItemTickMod() : mSelf(*ll::mod::NativeMod::current()) {}

    [[nodiscard]] ll::mod::NativeMod& getSelf() const { return mSelf; }

    bool load();
    bool enable();
    bool disable();

private:
    ll::mod::NativeMod& mSelf;
};

} // namespace parallel_item_tick
