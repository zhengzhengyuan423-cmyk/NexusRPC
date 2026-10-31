#pragma once
#include <cassert>
#include <functional>
#include "net/conn/socket.hpp"
#include "net/event/channel.hpp"
#include "net/event/event_loop.hpp"

namespace nexus {

// 监听套接字管理：accept 循环 + 新 fd 上交回调
class Acceptor
{
private:
    Socket _socket; // 监听套接字
    EventLoop *_loop;
    Channel _channel;

    using AcceptCallback = std::function<void(int)>;
    AcceptCallback _accept_callback;

private:
    // 一次取空 accept 队列：Accept 拿到 EAGAIN 才停。
    void HandleRead()
    {
        int newfd = -1;
        while ((newfd = _socket.Accept()) >= 0)
        {
            if (_accept_callback) _accept_callback(newfd);
        }
    }
    int CreateListen(int port)
    {
        // 监听 fd 必须非阻塞：否则取空循环里的 accept() 会永久挂起整个 EventLoop
        bool ret = _socket.Listen(port, "0.0.0.0", true);
        assert(ret == true);
        return _socket.Fd();
    }

public:
    Acceptor(EventLoop *loop, int port)
        : _socket(CreateListen(port)), _loop(loop), _channel(loop, _socket.Fd())
    {
        _channel.SetReadCallback(std::bind(&Acceptor::HandleRead, this));
    }
    void SetAcceptCallback(const AcceptCallback &cb) { _accept_callback = cb; }
    // 必须在设好回调之后启动监控：先启动则事件先到、回调未设，新连接白丢
    void Listen() { _channel.EnableRead(); }
    Channel *GetChannel() { return &_channel; }
};

} 
