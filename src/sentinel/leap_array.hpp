#pragma once
#include <atomic>
#include <cstdint>
#include <mutex>
#include <vector>

namespace nexus {

// 滑动窗口格子：归属时间戳 + 无锁计数。
// stamp 是格子归属的唯一凭证
class WindowBucket
{
public:
    std::atomic<int64_t> _start_ms{0};
    std::atomic<uint64_t> _count{0};
};

class LeapArray
{
public:
    LeapArray(int64_t interval_ms, int n);
    void Add(int64_t now_ms, uint64_t n = 1);
    // 当前槽往前 n 格求和；归属不符的格子（被轮转挤掉/时钟回拨）算 0
    uint64_t Sum(int64_t now_ms);
    // 全表作废清零（stamp=-1 匹配任何非负槽）。熔断半开通过后旧账不带入新周期用
    void Reset();

private:
    int64_t SlotOf(int64_t ms) const { return ms / _interval; }
    WindowBucket &At(int64_t slot)
    {
        int64_t idx = slot % _n;
        return _buckets[idx < 0 ? idx + _n : idx]; // Sum 会往回摸负槽号，环回合法
    }

    int64_t _interval;
    int _n;
    std::vector<WindowBucket> _buckets;
    std::mutex _reset_mutex; // 只串行化"翻新格子"慢路径，加数热点不排队
};

}
