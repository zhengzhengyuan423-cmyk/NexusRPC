#pragma once
#include <atomic>
#include <cstdint>
#include <mutex>
#include "sentinel/leap_array.hpp"

namespace nexus {

// QPS 限流器
// 判定不计数，放行的调用方负责 RecordPass。
class RateLimiter
{
public:
    RateLimiter(double qps, int warm_up_sec = 0);

    bool CanPass(int64_t now_ms);       // 纯读判定：不碰任何状态
    void RecordPass(int64_t now_ms);    // 唯一记账口：放行之后才 +1

    // 排队模式：TryReserve 先占位，返回等待时长；调用方等够了再 RecordPass。
    int64_t TryReserve(int64_t now_ms, int64_t max_queueing_ms);

    double ThresholdAt(int64_t now_ms); // 含 warm-up 升压的当前额度
    uint64_t Passed(int64_t now_ms);    // 窗口内已放行数

private:
    LeapArray _passes; // 1s 窗口：500ms × 2 格
    double _qps;
    int64_t _warm_up_ms;
    std::atomic<int64_t> _born_ms{-1}; // 首笔判定定 warm-up 起点
    std::mutex _reserve_mutex;
    int64_t _next_free_ms = 0; // 均匀排队：下一个可放行时刻
};

}
