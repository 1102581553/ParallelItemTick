// ParallelItemTick.h
#pragma once

#include <ll/api/Config.h>
#include <ll/api/io/Logger.h>
#include <ll/api/mod/NativeMod.h>

namespace parallel_item_tick {

struct Config {
    int  version        = 1;
    bool enabled        = true;
    bool debug          = false;
    bool stats          = true;
    int  workerThreads  = 0;     // 0 = auto (cpu cores - 1, clamped 1~7)
    int  minParallelCount = 16;  // 少于此数量不并行
    int  statsIntervalSec = 5;
};

Config& getConfig();
bool    loadConfig();
bool    saveConfig();

class ParallelItemTick {
public:
    static ParallelItemTick& getInstance();

    ParallelItemTick() : mSelf(*ll::mod::NativeMod::current()) {}

    [[nodiscard]] ll::mod::NativeMod& getSelf() const { return mSelf; }

    bool load();
    bool enable();
    bool disable();

private:
    ll::mod::NativeMod& mSelf;
};

} // namespace parallel_item_tick
