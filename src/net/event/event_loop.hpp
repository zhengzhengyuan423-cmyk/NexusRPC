#pragma once
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>
#include <cassert>
#include <cerrno>
#include <unistd.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#include "net/event/poller.hpp"
#include "net/event/channel.hpp"
#include "net/logger.hpp"

namespace nexus {

using TaskFunc = std::function<void()>;

// 每个 EventLoop 的运行指标。全部 relaxed 原子：只做监控计数，
// 不参与任何跨线程同步决策
struct LoopMetrics
{
    std::atomic<uint64_t> active_connections{0};
    std::atomic<uint64_t> total_connections{0};
    std::atomic<uint64_t> total_requests{0};
    std::atomic<uint64_t> total_bytes_read{0};
    std::atomic<uint64_t> total_bytes_written{0};
    std::atomic<uint64_t> rejected_connections{0};

    void OnConnectionCreated()
    {
        active_connections.fetch_add(1, std::memory_order_relaxed);
        total_connections.fetch_add(1, std::memory_order_relaxed);
    }
    void OnConnectionRejected() { rejected_connections.fetch_add(1, std::memory_order_relaxed); }
    void OnConnectionClosed() { active_connections.fetch_sub(1, std::memory_order_relaxed); }
    void OnRequest() { total_requests.fetch_add(1, std::memory_order_relaxed); }
    void OnBytesRead(uint64_t n) { total_bytes_read.fetch_add(n, std::memory_order_relaxed); }
    void OnBytesWritten(uint64_t n) { total_bytes_written.fetch_add(n, std::memory_order_relaxed); }

    // 可按值传递的快照。六个 load 不构成原子组，弱一致——监控场景够用
    struct Snapshot
    {
        uint64_t active_connections;
        uint64_t total_connections;
        uint64_t total_requests;
        uint64_t total_bytes_read;
        uint64_t total_bytes_written;
        uint64_t rejected_connections;
    };
    Snapshot GetSnapshot() const
    {
        return {active_connections.load(std::memory_order_relaxed),
                total_connections.load(std::memory_order_relaxed),
                total_requests.load(std::memory_order_relaxed),
                total_bytes_read.load(std::memory_order_relaxed),
                total_bytes_written.load(std::memory_order_relaxed),
                rejected_connections.load(std::memory_order_relaxed)};
    }
};

class TimerTask
{
private:
    uint64_t _id;      // 任务 id，同时是 TimerWheel 索引表的键
    uint32_t _timeout; // 延迟秒数
    TaskFunc _task_cb;
    bool _canceled;
    TaskFunc _release; // 销毁时从 TimerWheel 索引表摘除自己
public:
    TimerTask(uint64_t id, uint32_t delay, const TaskFunc &cb)
        : _id(id), _timeout(delay), _task_cb(cb), _canceled(false) {}
    ~TimerTask()
    {
        // 触发=析构：未取消才跑任务；_release 无条件走（摘索引）
        if (_canceled == false)
            _task_cb();
        if (_release)
            _release();
    }
    void Cancel() { _canceled = true; }
    void SetRelease(const TaskFunc &cb) { _release = cb; }
    uint32_t DelayTime() { return _timeout; }
};

// 60 格定时轮，timerfd 每 1s 走一格，到期格 clear() 析构任务。
// delay>=60 会绕格提前误触发——容量即最大延迟，超界不排队
class TimerWheel
{
private:
    using WeakTask = std::weak_ptr<TimerTask>;
    using PtrTask = std::shared_ptr<TimerTask>;
    int _capacity; // 表盘格数 == 最大延迟秒数
    int _tick;     // 当前秒针位置
    std::vector<std::vector<PtrTask>> _wheel;
    std::unordered_map<uint64_t, WeakTask> _timers;

    EventLoop *_loop;
    int _timerfd;
    std::unique_ptr<Channel> _timer_channel;

private:
    void RemoveTimer(uint64_t id)
    {
        auto it = _timers.find(id);
        if (it != _timers.end())
        {
            _timers.erase(it);
        }
    }
    static int CreateTimerfd()
    {
        int timerfd = timerfd_create(CLOCK_MONOTONIC, 0);
        if (timerfd < 0)
        {
            ELOG << "TIMERFD CREATE FAILED!";
            abort();
        }
        struct itimerspec itime;
        itime.it_value.tv_sec = 1;
        itime.it_value.tv_nsec = 0;
        itime.it_interval.tv_sec = 1;
        itime.it_interval.tv_nsec = 0;
        timerfd_settime(timerfd, 0, &itime, NULL);
        return timerfd;
    }
    // 读出距上次以来超时次数：慢回调会攒多次，补走对应格数
    int ReadTimerfd()
    {
        uint64_t times;
        int ret = read(_timerfd, &times, 8);
        if (ret < 0)
        {
            if (errno == EINTR) return 0;
            ELOG << "READ TIMERFD FAILED!";
            abort();
        }
        return times;
    }
    void RunTimerTask()
    {
        _tick = (_tick + 1) % _capacity;
        _wheel[_tick].clear(); // 析构该格所有 TimerTask == 到期触发
    }
    void OnTime()
    {
        int times = ReadTimerfd();
        for (int i = 0; i < times; i++)
        {
            RunTimerTask();
        }
    }
    void TimerAddInLoop(uint64_t id, uint32_t delay, const TaskFunc &cb)
    {
        PtrTask pt(new TimerTask(id, delay, cb));
        pt->SetRelease(std::bind(&TimerWheel::RemoveTimer, this, id));
        int pos = (_tick + delay) % _capacity;
        _wheel[pos].push_back(pt);
        _timers[id] = WeakTask(pt);
    }
    // 刷新=重新挂一格：旧格的 clear() 只让 weak_ptr 过期不触发（引用计数>0），
    // 任务本体被 _timers 的 weak 与格内 shared 同时观察
    void TimerRefreshInLoop(uint64_t id)
    {
        auto it = _timers.find(id);
        if (it == _timers.end())
        {
            return;
        }
        PtrTask pt = it->second.lock();
        if (!pt) return;
        int delay = pt->DelayTime();
        int pos = (_tick + delay) % _capacity;
        _wheel[pos].push_back(pt);
    }
    void TimerCancelInLoop(uint64_t id)
    {
        auto it = _timers.find(id);
        if (it == _timers.end())
        {
            return;
        }
        PtrTask pt = it->second.lock();
        if (pt)
            pt->Cancel(); // 只打标记：真摘除仍等旧格 clear()
    }

public:
    TimerWheel(EventLoop *loop);
    // 析构必须先清轮再死索引表：~TimerTask 会回调 RemoveTimer 碰 _timers
    ~TimerWheel();
    void TimerAdd(uint64_t id, uint32_t delay, const TaskFunc &cb);
    void TimerRefresh(uint64_t id);
    void TimerCancel(uint64_t id);
    // 只允许在属主 EventLoop 线程内调用（直读 _timers 无锁）
    bool HasTimer(uint64_t id);
};

// 单线程 reactor 循环体。约定：在哪个线程构造，就在哪个线程 Start，
// 事件回调与任务池全部在该线程执行（线程亲和）
class EventLoop
{
private:
    std::thread::id _thread_id;
    int _event_fd; // 跨线程投递的自唤醒通道
    std::unique_ptr<Channel> _event_channel;
    Poller _poller;
    std::vector<TaskFunc> _tasks;
    std::mutex _mutex;
    TimerWheel _timer_wheel;
    std::atomic<bool> _running{true};
    LoopMetrics _metrics;

private:
    static int CreateEventFd()
    {
        int efd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
        if (efd < 0)
        {
            ELOG << "CREATE EVENTFD FAILED!";
            abort();
        }
        return efd;
    }
    // EAGAIN=已被自己顺手排干，正常；其余是硬错误
    void ReadEventfd()
    {
        uint64_t res = 0;
        int ret = read(_event_fd, &res, sizeof(res));
        if (ret < 0)
        {
            if (errno == EINTR || errno == EAGAIN)
            {
                return;
            }
            ELOG << "READ EVENTFD FAILED!";
            abort();
        }
    }
    void Wakeup()
    {
        uint64_t val = 1;
        int ret = write(_event_fd, &val, sizeof(val));
        if (ret < 0)
        {
            if (errno == EINTR)
            {
                return;
            }
            ELOG << "WRITE EVENTFD FAILED!";
            abort();
        }
    }
    // 事件监控-》派发就绪-》执行任务池
    void RunAllTask()
    {
        std::vector<TaskFunc> functor;
        {
            std::unique_lock<std::mutex> _lock(_mutex);
            functor.swap(_tasks); // swap 交接：锁只护入队，不护执行
        }
        for (auto &f : functor)
        {
            f();
        }
    }

public:
    EventLoop();
    void Start();
    // 任意线程可调：置停+跨线程时踢 eventfd；当轮任务池仍会执行完才退出
    void Stop();
    bool IsInLoop() { return (_thread_id == std::this_thread::get_id()); }
    void AssertInLoop() { assert(_thread_id == std::this_thread::get_id()); }
    // 同线程就地执行，异线程入队并唤醒
    void RunInLoop(const TaskFunc &cb);
    // 永远入队（供 Release 这类"必然后置"的操作使用）
    void QueueInLoop(const TaskFunc &cb);
    void UpdateEvent(Channel *channel) { _poller.UpdateEvent(channel); }
    void RemoveEvent(Channel *channel) { _poller.RemoveEvent(channel); }
    void TimerAdd(uint64_t id, uint32_t delay, const TaskFunc &cb) { _timer_wheel.TimerAdd(id, delay, cb); }
    void TimerRefresh(uint64_t id) { _timer_wheel.TimerRefresh(id); }
    void TimerCancel(uint64_t id) { _timer_wheel.TimerCancel(id); }
    bool HasTimer(uint64_t id) { return _timer_wheel.HasTimer(id); }
    LoopMetrics &GetMetrics() { return _metrics; }
};

}
