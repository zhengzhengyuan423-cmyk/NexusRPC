#pragma once
#include <atomic>
#include <cstdint>
#include <mutex>
#include "sentinel/leap_array.hpp"

namespace nexus {

enum class CBState
{
    CLOSED,
    OPEN,
    HALF_OPEN,
};

class CircuitBreaker
{
public:
    // slow_rt_ms>0 才启用慢调用策略；窗口统一 1s（500ms×2 格）
    CircuitBreaker(double ex_ratio, int64_t slow_rt_ms, double slow_ratio,
                   int min_request, int64_t recovery_ms);

    bool Allow(int64_t now_ms);
    void RecordResult(int64_t now_ms, bool success, int64_t rt_ms);
    CBState CurState(int64_t now_ms);

private:
    void TripOpen(int64_t now_ms);

    LeapArray _total;
    LeapArray _fail;
    LeapArray _slow;
    double _ex_ratio;
    int64_t _slow_rt_ms;
    double _slow_ratio;
    int _min_request;
    int64_t _recovery_ms;

    std::mutex _state_mutex; // 只串行化状态迁移（开窗/拉半开），记账不排队
    CBState _state = CBState::CLOSED;
    int64_t _open_until = 0;
    std::atomic<int> _probe_left{0};
};

}
