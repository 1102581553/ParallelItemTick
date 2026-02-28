// ParallelItemTick.h
#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_set>
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

class TickWorkerPool {
public:
    explicit TickWorkerPool(int numWorkers);
    ~TickWorkerPool();

    TickWorkerPool(TickWorkerPool const&)            = delete;
    TickWorkerPool& operator=(TickWorkerPool const&) = delete;

    void parallelFor(size_t count, std::function<void(size_t)> const& func);

private:
    void workerMain();

    int                      mNumWorkers;
    std::vector<std::thread> mWorkers;

    std::mutex              mMutex;
    std::condition_variable mCvWork;
    std::condition_variable mCvDone;

    std::function<void(size_t)> const* mWorkFunc{nullptr};
    std::atomic<size_t>                mNextIndex{0};
    size_t                             mWorkCount{0};
    int                                mActiveWorkers{0};
    bool                               mShutdown{false};
};

extern Config                          gConfig;
extern std::shared_ptr<ll::io::Logger> gLogger;
extern bool                            gStatsRunning;
extern std::unique_ptr<TickWorkerPool> gWorkerPool;

// 已被并行处理过的 ItemActor 指针集合
// 只在主线程读写（phase3 串行阶段），无需加锁
extern std::unordered_set<void*> gProcessedActors;

thread_local extern bool gSuppressMerge;

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
