#pragma once
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>
#include "net/event/event_loop.hpp"

namespace nexus {

// 一条持有 EventLoop 的线程。EventLoop 在本线程构造（线程亲和成立），
// 但所有权在 LoopThread：线程退出后对象仍存活，loop 指针到 ~LoopThread 才失效
class LoopThread
{
private:
    std::mutex _mutex;
    std::condition_variable _cond; // 发布 loop 指针的同步原语
    EventLoop *_loop;
    std::unique_ptr<EventLoop> _owned;
    std::thread _thread;

private:
    void ThreadEntry()
    {
        EventLoop *loop = new EventLoop(); // 在本线程内构造：_thread_id 记录的就是 worker 线程
        {
            std::unique_lock<std::mutex> lock(_mutex);
            _owned.reset(loop);
            _loop = loop;
            _cond.notify_all();
        }
        loop->Start();
        // Start 返回后不清 _loop：对象活着，关停后读它的指标依然合法
    }

public:
    LoopThread() : _loop(NULL), _thread(std::thread(&LoopThread::ThreadEntry, this)) {}
    ~LoopThread()
    {
        {
            std::lock_guard<std::mutex> lock(_mutex);
            if (_loop)
            {
                _loop->Stop();
            }
        }
        if (_thread.joinable())
        {
            _thread.join();
        }
        // join 完成后 _owned 随本对象析构 loop
    }
    // 等 loop 线程退出（Start 已返回）。对象析构前调，确保排空回调跑完时宿主还活着。
    // 必须先 Stop：Start 只有看到 _running 为假才返回，光 join 会死等
    void Join()
    {
        {
            std::lock_guard<std::mutex> lock(_mutex);
            if (_loop)
                _loop->Stop();
        }
        if (_thread.joinable())
            _thread.join();
    }
    // 阻塞直到 worker 线程发布 loop 指针；返回的指针在本对象存活期内有效
    EventLoop *GetLoop()
    {
        EventLoop *loop = NULL;
        {
            std::unique_lock<std::mutex> lock(_mutex);
            _cond.wait(lock, [&]()
                       { return _loop != NULL; });
            loop = _loop;
        }
        return loop;
    }
};

// 从属 EventLoop 线程池：连接按轮转分给各 loop，一连接一 loop 终生不变
class LoopThreadPool
{
private:
    int _thread_count;
    int _next_idx;
    EventLoop *_baseloop;
    std::vector<LoopThread *> _threads;
    std::vector<EventLoop *> _loops;

public:
    LoopThreadPool(EventLoop *baseloop) : _thread_count(0), _next_idx(0), _baseloop(baseloop) {}
    ~LoopThreadPool()
    {
        for (auto t : _threads)
            delete t;
    }
    void SetThreadCount(int count) { _thread_count = count; }
    // 只在 baseloop.Start 之前调一次；之后 _threads/_loops 只读，全读共享免锁
    void Create()
    {
        if (_thread_count > 0)
        {
            _threads.resize(_thread_count);
            _loops.resize(_thread_count);
            for (int i = 0; i < _thread_count; i++)
            {
                _threads[i] = new LoopThread();
                _loops[i] = _threads[i]->GetLoop();
            }
        }
        return;
    }
    EventLoop *NextLoop()
    {
        if (_thread_count == 0)
        {
            return _baseloop; // count=0 隐藏形态：baseloop 一身兼二职
        }
        EventLoop *loop = _loops[_next_idx];
        _next_idx = (_next_idx + 1) % _thread_count;
        return loop;
    }
    const std::vector<EventLoop *> &Loops() const { return _loops; }
    void Stop()
    {
        for (EventLoop *l : _loops)
            l->Stop();
    }
};

} 
