#pragma once
#include <functional>
#include <cstdint>
#include <sys/epoll.h>

namespace nexus {

class EventLoop;

// Channel：一个 fd 的事件注册与派发，生命周期归属其 EventLoop 线程
class Channel
{
private:
    int _fd;
    uint32_t _revents; // 就绪事件
    EventLoop *_loop;
    uint32_t _events;  // 兴趣事件
    using EventCallback = std::function<void()>;
    EventCallback _read_callback;
    EventCallback _write_callback;
    EventCallback _error_callback;
    EventCallback _close_callback;
    EventCallback _event_callback; // 任意事件都派发，用于活跃刷新等旁路观察

public:
    Channel(EventLoop *loop, int fd) : _fd(fd), _revents(0), _loop(loop), _events(0) {}
    int Fd() { return _fd; }
    uint32_t Events() { return _events; }
    void SetREvents(uint32_t events) { _revents = events; }
    void SetReadCallback(const EventCallback &cb) { _read_callback = cb; }
    void SetWriteCallback(const EventCallback &cb) { _write_callback = cb; }
    void SetErrorCallback(const EventCallback &cb) { _error_callback = cb; }
    void SetCloseCallback(const EventCallback &cb) { _close_callback = cb; }
    void SetEventCallback(const EventCallback &cb) { _event_callback = cb; }

    bool Readable() { return (_events & EPOLLIN); }
    bool Writable() { return (_events & EPOLLOUT); }
    void EnableRead() { _events |= EPOLLIN; Update(); }
    void EnableWrite() { _events |= EPOLLOUT; Update(); }
    void DisableRead() { _events &= ~EPOLLIN; Update(); }
    void DisableWrite() { _events &= ~EPOLLOUT; Update(); }
    void DisableAll() { _events = 0; Update(); }
    // 注册/摘除 epoll 的定义在 event_loop.cpp：需要 EventLoop 完整类型
    void Remove();
    void Update();

    void HandleEvent()
    {
        // 快照就绪事件：回调可能摘除/修改本 Channel，后续分支不再读成员
        const uint32_t revents = _revents;
        _revents = 0;
        if (revents & EPOLLIN)
        {
            if (_read_callback) _read_callback();
        }
        if (revents & EPOLLOUT)
        {
            if (_write_callback) _write_callback();
        }
        if (revents & EPOLLERR)
        {
            if (_error_callback) _error_callback();
        }
        // HUP 不提前 return：对端 close 常伴随 IN|RDNORM，EOF 先派发读再走 close，一条不丢
        if (revents & EPOLLHUP)
        {
            if (_close_callback) _close_callback();
        }
        if (_event_callback) _event_callback();
    }
};

} 
