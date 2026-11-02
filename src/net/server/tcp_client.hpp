#pragma once
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include "net/conn/connection.hpp"
#include "net/server/loop_thread.hpp"

namespace nexus {

// 单连接客户端：独占 LoopThread，阻塞式 connect，不做自动重连。

class TcpClient
{
public:
    using ConnectedCallback = std::function<void(const PtrConnection &)>;
    using MessageCallback = std::function<void(const PtrConnection &, std::string)>;
    using ClosedCallback = std::function<void(const PtrConnection &)>;

    TcpClient(const std::string &ip, uint16_t port);
    ~TcpClient();

    void SetConnectedCallback(const ConnectedCallback &cb) { _connected_callback = cb; }
    void SetMessageCallback(const MessageCallback &cb) { _message_callback = cb; }
    void SetClosedCallback(const ClosedCallback &cb) { _closed_callback = cb; }

    bool Connect();
    PtrConnection GetConn() { return _conn; }
    EventLoop *GetLoop() { return _loop_thread.GetLoop(); }
    // 同步收摊：发 FIN→排空→join，返回后该连接不再有任何回调
    void Shutdown();

private:
    std::string _ip;
    uint16_t _port;
    LoopThread _loop_thread; // 先声明：loop 必须活得比挂在它上面的连接久
    PtrConnection _conn;
    ConnectedCallback _connected_callback;
    MessageCallback _message_callback;
    ClosedCallback _closed_callback;
};

}
