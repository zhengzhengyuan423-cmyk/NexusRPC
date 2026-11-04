#include "sentinel/leap_array.hpp"

namespace nexus {

LeapArray::LeapArray(int64_t interval_ms, int n)
    : _interval(interval_ms), _n(n), _buckets(n)
{
}

void LeapArray::Add(int64_t now_ms, uint64_t n)
{
    int64_t slot = SlotOf(now_ms);
    WindowBucket &b = At(slot);
    if (b._start_ms.load(std::memory_order_acquire) == slot * _interval)
    {
        b._count.fetch_add(n, std::memory_order_relaxed);
        return;
    }
    std::lock_guard<std::mutex> lk(_reset_mutex);
    int64_t want = slot * _interval;
    if (b._start_ms.load(std::memory_order_acquire) != want)
    {
        b._count.store(0, std::memory_order_relaxed);
        b._start_ms.store(want, std::memory_order_release);
    }
    b._count.fetch_add(n, std::memory_order_relaxed);
}

uint64_t LeapArray::Sum(int64_t now_ms)
{
    int64_t slot = SlotOf(now_ms);
    uint64_t total = 0;
    for (int64_t k = 0; k < _n; k++)
    {
        WindowBucket &b = At(slot - k);
        if (b._start_ms.load(std::memory_order_acquire) == (slot - k) * _interval)
            total += b._count.load(std::memory_order_relaxed);
    }
    return total;
}

void LeapArray::Reset()
{
    std::lock_guard<std::mutex> lk(_reset_mutex);
    for (WindowBucket &b : _buckets)
    {
        b._count.store(0, std::memory_order_relaxed);
        b._start_ms.store(-1, std::memory_order_release);
    }
}

}
