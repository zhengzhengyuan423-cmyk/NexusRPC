#include "sentinel/rate_limiter.hpp"

namespace nexus {

RateLimiter::RateLimiter(double qps, int warm_up_sec)
    : _passes(500, 2), _qps(qps), _warm_up_ms((int64_t)warm_up_sec * 1000)
{
}

// qps>=1。warm_up_sec>0：额度从 qps/3 起，线性升压到 qps
double RateLimiter::ThresholdAt(int64_t now_ms)
{
    if (_warm_up_ms <= 0)
        return _qps;
    int64_t expect = -1;
    _born_ms.compare_exchange_strong(expect, now_ms);
    int64_t elapsed = now_ms - _born_ms.load();
    if (elapsed >= _warm_up_ms)
        return _qps;
    double floor_qps = _qps / 3.0;
    return floor_qps + (_qps - floor_qps) * (double)elapsed / (double)_warm_up_ms;
}

bool RateLimiter::CanPass(int64_t now_ms)
{
    return (double)_passes.Sum(now_ms) < ThresholdAt(now_ms);
}

void RateLimiter::RecordPass(int64_t now_ms)
{
    _passes.Add(now_ms);
}

uint64_t RateLimiter::Passed(int64_t now_ms)
{
    return _passes.Sum(now_ms);
}

int64_t RateLimiter::TryReserve(int64_t now_ms, int64_t max_queueing_ms)
{
    double thr = ThresholdAt(now_ms);
    int64_t gap = (int64_t)(1000.0 / thr); // 均匀间隔；预算>=1000qps 时 0=不排队
    std::lock_guard<std::mutex> lk(_reserve_mutex);
    if (now_ms + max_queueing_ms < _next_free_ms)
        return -1; // 排到槽位要等的比预算长：不排队，直接拒
    int64_t wait = _next_free_ms > now_ms ? _next_free_ms - now_ms : 0;
    int64_t base = _next_free_ms > now_ms ? _next_free_ms : now_ms;
    _next_free_ms = base + gap;
    RecordPass(now_ms); // 预约即占额度：这是占位动作，不是判定
    return wait;
}

}
