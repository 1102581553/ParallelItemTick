// ParallelItemTick.h
#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
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
};

extern Config                          gConfig;
extern std::shared_ptr<ll::io::Logger> gLogger;
extern bool                            gStatsRunning;

extern std::atomic<uint64_t> gTotalTicks;
extern std::atomic<uint64_t> gTotalProcessed;
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
