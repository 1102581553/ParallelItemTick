#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace ll::io {
class Logger;
}

namespace parallel_item_tick {

// ============================================================
// 配置结构体
// ============================================================
struct Config {
    int  version        = 1;          // 配置文件版本
    bool enabled        = true;       // 总开关
    bool debug          = false;      // 每 tick 打印处理的掉落物数量及耗时
    bool stats          = true;       // 每 5 秒输出统计信息，默认开启
    int  minParallelCnt = 16;         // 并行阈值，少于该数量不并行
    int  workerThreads  = 0;          // 工作线程数，0 表示自动检测 (cpu 核心数 -1)，最大 7
};

// ============================================================
// 线程池声明
// ============================================================
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

// ============================================================
// 全局变量声明 (外部链接)
// ============================================================
extern Config                    gConfig;
extern std::shared_ptr<ll::io::Logger> gLogger;
extern bool                       gStatsRunning;
extern std::unique_ptr<TickWorkerPool> gWorkerPool;

// 线程局部变量
extern thread_local bool gSuppressMerge;

// 全局标志
extern bool gSkipProcessedItems;

// ============================================================
// 统计变量声明
// ============================================================
extern std::atomic<uint64_t> gTotalTicks;
extern std::atomic<uint64_t> gTotalProcessed;
extern std::atomic<uint64_t> gTotalMerged;
extern std::atomic<uint64_t> gTotalTimeUs;
extern std::atomic<uint64_t> gMaxTimeUs;

// ============================================================
// 函数声明
// ============================================================
ll::io::Logger& getLogger();

bool loadConfig();
bool saveConfig();

void startStatsTask();
void stopStatsTask();

// ============================================================
// 插件主类声明
// ============================================================
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
