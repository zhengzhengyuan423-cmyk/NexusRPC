#pragma once
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include "sentinel/circuit_breaker.hpp"
#include "sentinel/rate_limiter.hpp"

namespace nexus {

class Sentinel
{
public:
    struct LimiterCfg
    {
        bool enabled = false;
        double qps = 100.0;
        int warm_up_sec = 0;
        int64_t queueing_ms = 0; // >0 走 TryReserve 排队报等待；0 纯 FAST_FAIL
    };
    struct BreakerCfg
    {
        bool enabled = false;
        double ex_ratio = 0.5;
        int64_t slow_rt_ms = -1; // >0 启用慢调用策略
        double slow_ratio = 0.5;
        int min_request = 5;
        int64_t recovery_ms = 5000;
    };

    // 服务启动前登记
    void Configure(const std::string &method, const LimiterCfg &lmt,
                   const BreakerCfg &bkr);
    void Configure(const std::string &method, const LimiterCfg &lmt);

    // 判定链
    bool Entry(const std::string &method, int64_t now_ms, int64_t &wait_ms);
    // 只允许在 Entry 成功后调用
    void Exit(const std::string &method, bool success, int64_t rt_ms, int64_t now_ms);

private:
    struct Chain
    {
        std::shared_ptr<RateLimiter> limiter;
        std::shared_ptr<CircuitBreaker> breaker;
        int64_t queueing_ms = 0;
    };
    std::unordered_map<std::string, Chain> _chains;
};

}
