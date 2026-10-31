#pragma once
#include <atomic>
#include <cassert>
#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace nexus {

// 业务线程池：把慢业务从 io 线程摘出去。
// 队列本身跨线程（io 生产、biz 消费），mutex+condvar 不可少
class BizThreadPool
{
private:
    struct TaskQueue
    {
        std::mutex _mutex;
        std::condition_variable _cond; // 队列空时让 worker 睡觉
        std::deque<std::function<void()>> _tasks;
        bool _running = true;
    };
    int _thread_count;
    size_t _queue_size;
    std::atomic<uint64_t> _dropped; // 满丢计数
    std::deque<TaskQueue> _queues;  // 元素地址稳定且可下标访问，装得下带 mutex 的不可拷贝结构
    std::vector<std::thread> _threads;

    void WorkerEntry(int idx)
    {
        TaskQueue &q = _queues[idx];
        while (true)
        {
            std::unique_lock<std::mutex> lock(q._mutex);
            q._cond.wait(lock, [&]()
                         { return !q._running || !q._tasks.empty(); });
            if (!q._running) return; // 关停优先：未跑的积压任务随容器析构丢弃
            std::function<void()> task = std::move(q._tasks.front());
            q._tasks.pop_front();
            lock.unlock(); // 执行不持锁：慢任务不许堵住别人的 Post
            task();
        }
    }

public:
    BizThreadPool() : _thread_count(0), _queue_size(1024), _dropped(0) {}
    ~BizThreadPool() { Stop(); }
    void SetThreadCount(int count) { _thread_count = count; }
    void SetQueueSize(size_t size) { _queue_size = size; }
    // 真起线程；约定同 LoopThreadPool：Start 前一次，之后不重建
    void Create()
    {
        _queues.clear();
        _threads.clear();
        for (int i = 0; i < _thread_count; i++)
            _queues.emplace_back();
        for (int i = 0; i < _thread_count; i++)
            _threads.emplace_back(&BizThreadPool::WorkerEntry, this, i);
    }
    // 返回 false == 队列满被丢。供 io 线程调用
    bool Post(uint64_t conn_id, const std::function<void()> &task)
    {
        assert(_thread_count > 0);
        TaskQueue &q = _queues[conn_id % _thread_count];
        {
            std::lock_guard<std::mutex> lock(q._mutex);
            if (q._tasks.size() >= _queue_size)
            {
                _dropped.fetch_add(1, std::memory_order_relaxed);
                return false;
            }
            q._tasks.push_back(task);
        }
        q._cond.notify_one(); // 锁外 notify：省一次"醒了发现锁没放"的切换
        return true;
    }
    uint64_t Dropped() const { return _dropped.load(std::memory_order_relaxed); }
    // 置停+叫醒+join 等手头任务跑完；幂等
    void Stop()
    {
        for (auto &q : _queues)
        {
            std::lock_guard<std::mutex> lock(q._mutex);
            q._running = false;
            q._cond.notify_all();
        }
        for (auto &t : _threads)
            if (t.joinable()) t.join();
        _threads.clear();
    }
};

} 