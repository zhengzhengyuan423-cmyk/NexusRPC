#include "sentinel/sentinel.hpp"

namespace nexus {

void Sentinel::Configure(const std::string &method, const LimiterCfg &lmt,
                         const BreakerCfg &bkr)
{
    Chain c;
    if (lmt.enabled)
        c.limiter = std::make_shared<RateLimiter>(lmt.qps, lmt.warm_up_sec);
    if (bkr.enabled)
        c.breaker = std::make_shared<CircuitBreaker>(
            bkr.ex_ratio, bkr.slow_rt_ms, bkr.slow_ratio, bkr.min_request, bkr.recovery_ms);
    c.queueing_ms = lmt.queueing_ms;
    _chains[method] = std::move(c);
}

void Sentinel::Configure(const std::string &method, const LimiterCfg &lmt)
{
    Configure(method, lmt, BreakerCfg());
}

bool Sentinel::Entry(const std::string &method, int64_t now_ms, int64_t &wait_ms)
{
    wait_ms = 0;
    auto it = _chains.find(method);
    if (it == _chains.end())
        return true; // 治理关：直通
    Chain &c = it->second;
    if (c.limiter)
    {
        if (c.queueing_ms > 0)
        {
            int64_t w = c.limiter->TryReserve(now_ms, c.queueing_ms);
            if (w < 0)
                return false;
            wait_ms = w; // 额度已预约占用，Exit 不再记限流账
        }
        else if (!c.limiter->CanPass(now_ms))
            return false;
    }
    if (c.breaker && !c.breaker->Allow(now_ms))
        return false;
    return true;
}

void Sentinel::Exit(const std::string &method, bool success, int64_t rt_ms, int64_t now_ms)
{
    auto it = _chains.find(method);
    if (it == _chains.end())
        return;
    Chain &c = it->second;
    if (c.limiter && c.queueing_ms <= 0)
        c.limiter->RecordPass(now_ms); // 判定不计数，账记在放行方
    if (c.breaker)
        c.breaker->RecordResult(now_ms, success, rt_ms);
}

}
