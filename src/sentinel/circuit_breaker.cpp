#include "sentinel/circuit_breaker.hpp"

namespace nexus {

CircuitBreaker::CircuitBreaker(double ex_ratio, int64_t slow_rt_ms, double slow_ratio,
                               int min_request, int64_t recovery_ms)
    : _total(500, 2), _fail(500, 2), _slow(500, 2), _ex_ratio(ex_ratio),
      _slow_rt_ms(slow_rt_ms), _slow_ratio(slow_ratio), _min_request(min_request),
      _recovery_ms(recovery_ms)
{
}

bool CircuitBreaker::Allow(int64_t now_ms)
{
    std::unique_lock<std::mutex> lk(_state_mutex);
    if (_state == CBState::CLOSED)
        return true;
    if (_state == CBState::OPEN)
    {
        if (now_ms < _open_until)
            return false;
        _state = CBState::HALF_OPEN; // 到期第一个踏进来的人拉闸换态
        _probe_left = 1;
    }
    int expect = 1;
    // 半开只发一张票：CAS 抢 1→0，赢者放行，其余等探测结果
    return _probe_left.compare_exchange_strong(expect, 0);
}

void CircuitBreaker::RecordResult(int64_t now_ms, bool success, int64_t rt_ms)
{
    {
        std::lock_guard<std::mutex> lk(_state_mutex);
        if (_state == CBState::HALF_OPEN)
        {
            bool probe_bad = !success || (_slow_rt_ms > 0 && rt_ms >= _slow_rt_ms);
            if (probe_bad)
            {
                _state = CBState::OPEN;
                _open_until = now_ms + _recovery_ms;
                _probe_left = 0;
            }
            else
            {
                _total.Reset();
                _fail.Reset();
                _slow.Reset();
                _state = CBState::CLOSED;
            }
            return; // 探测样本不进窗口：新周期从零开始判
        }
        if (_state != CBState::CLOSED)
            return; 
    }
    _total.Add(now_ms);
    if (!success)
        _fail.Add(now_ms);
    if (_slow_rt_ms > 0 && rt_ms >= _slow_rt_ms)
        _slow.Add(now_ms);

    uint64_t total = _total.Sum(now_ms);
    if ((int64_t)total < _min_request)
        return; // 样本不足，比例不可信：一单失败不许跳闸
    bool ex_trip = (double)_fail.Sum(now_ms) >= _ex_ratio * (double)total;
    bool slow_trip = _slow_rt_ms > 0 &&
                     (double)_slow.Sum(now_ms) >= _slow_ratio * (double)total;
    if (ex_trip || slow_trip)
        TripOpen(now_ms);
}

CBState CircuitBreaker::CurState(int64_t now_ms)
{
    std::lock_guard<std::mutex> lk(_state_mutex);
    if (_state == CBState::OPEN && now_ms >= _open_until)
        return CBState::HALF_OPEN; 
    return _state;
}

void CircuitBreaker::TripOpen(int64_t now_ms)
{
    std::lock_guard<std::mutex> lk(_state_mutex);
    _state = CBState::OPEN;
    _open_until = now_ms + _recovery_ms;
    _probe_left = 0;
}

}
