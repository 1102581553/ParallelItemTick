// ParallelItemTick.h
#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
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

// 使用 mutex+condvar 的安全线程池
class TickWorkerPool {
public:
    explicit TickWorkerPool(int numWorkers);
    ~TickWorkerPool();

    TickWorkerPool(TickWorkerPool const&)            = delete;
    TickWorkerPool& operator=(TickWorkerPool const&) = delete;

    // 并行执行 count 个任务，全部完成后返回
    void parallelFor(size_t count, std::function<void(size_t)> const& func);

private:
    void workerMain();

    int                      mNumWorkers;
    std::vector<std::thread> mWorkers;

    std::mutex              mMutex;
    std::condition_variable mCvWork;   // 通知工作线程有任务
    std::condition_variable mCvDone;   // 通知主线程任务完成

    std::function<void(size_t)> const* mWorkFunc{nullptr};
    std::atomic<size_t>                mNextIndex{0};
    size_t                             mWorkCount{0};
    int                                mActiveworkers{0}; // 还在工作的线程数
    bool                               mShutdown{false};
};

extern Config                          gConfig;
extern std::shared_ptr<ll::io::Logger> gLogger;
extern bool                            gStatsRunning;
extern std::unique_ptr<TickWorkerPool> gWorkerPool;

extern thread_local bool gSuppressMerge;
extern bool              gSkipProcessedItems;

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
