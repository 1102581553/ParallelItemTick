// ParallelItemTick.h (修复版)
#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <latch>
#include <vector>

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

    TickWorkerPool(TickWorkerPool const&) = delete;
    TickWorkerPool& operator=(TickWorkerPool const&) = delete;

    void parallelFor(size_t count, std::function<void(size_t)> const& func);

    static constexpr size_t mMinParallelThreshold = 16;

private:
    void workerMain(int id);
    void executeWork();

    int                                    mNumWorkers;
    std::vector<std::thread>               mWorkers;
    std::atomic<bool>                      mShutdown{false};
    std::atomic<uint64_t>                  mGeneration{0};
    std::function<void(size_t)> const*     mWorkFunc{nullptr};
    size_t                                 mWorkCount{0};
    std::atomic<size_t>                    mNextIndex{0};
    std::latch*                            mDoneLatch{nullptr};
};

extern Config                         gConfig;
extern std::shared_ptr<ll::io::Logger> gLogger;
extern bool                           gStatsRunning;
extern std::unique_ptr<TickWorkerPool> gWorkerPool;

extern thread_local bool gSuppressMerge;
extern bool              gSkipProcessedItems;

extern std::atomic<uint64_t> gTotalTicks;
extern std::atomic<uint64_t> gTotalProcessed;
extern std::atomic<uint64_t> gTotalMerged;
extern std::atomic<uint64_t> gTotalTimeUs;
extern std::atomic<uint64_t> gMaxTimeUs;

ll::io::Logger& getLogger();
bool loadConfig();
bool saveConfig();
void startStatsTask();
void stopStatsTask();

class ParallelItemTickMod {
public:
    static ParallelItemTickMod& getInstance();

    ParallelItemTickMod();


    [[nodiscard]] ll::mod::NativeMod& getSelf() const;

    bool load();
    bool enable();
    bool disable();

private:
    ll::mod::NativeMod& mSelf;
};

} // namespace parallel_item_tick
