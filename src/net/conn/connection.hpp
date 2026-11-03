#pragma once
#include <memory>
#include <functional>
#include "net/conn/socket.hpp"
#include "net/conn/buffer.hpp"
#include "net/event/channel.hpp"
#include "net/event/event_loop.hpp"
#include "logger.hpp"

namespace nexus {

class Connection;
// DISCONNECTED 已关闭；CONNECTING 已建待初始化；
// CONNECTED 就绪可通信；DISCONNECTING 待关闭（先把待发冲完）
enum ConnState
{
    DISCONNECTED,
    CONNECTING,
    CONNECTED,
    DISCONNECTING
};

using PtrConnection = std::shared_ptr<Connection>;

// 一条 TCP 连接：fd+双缓冲+事件管理。全部内务方法在属主 loop 线程执行，公共接口是"壳投递"，任意线程可调
class Connection : public std::enable_shared_from_this<Connection>
{
private:
    uint64_t _conn_id; // 连接唯一 ID，兼作定时器 ID
    bool _idle_timer;  // 非活跃超时销毁是否已启动
    EventLoop *_loop;
    ConnState _statu;
    Socket _socket;   // fd 所有者，Fd() 从这里拿
    Channel _channel;
    Buffer _in_buffer;  // socket 读进来的待处理数据
    Buffer _out_buffer; // 已提交待发送的数据

    using ConnectedCallback = std::function<void(const PtrConnection &)>;
    using MessageCallback = std::function<void(const PtrConnection &, Buffer *)>;
    using ClosedCallback = std::function<void(const PtrConnection &)>;
    using AnyEventCallback = std::function<void(const PtrConnection &)>;
    ConnectedCallback _connected_callback;
    MessageCallback _message_callback;
    ClosedCallback _closed_callback;
    AnyEventCallback _event_callback;
    // 组件内部回调：连接释放时从 TcpServer 的管理表摘除自己
    ClosedCallback _server_closed_callback;

private:
    void HandleRead();
    void HandleWrite();
    void HandleClose();
    void HandleError() { return HandleClose(); }
    // 任意事件：刷活跃度 + 旁路回调
    void HandleEvent();

    void EstablishedInLoop();
    void ReleaseInLoop();
    void SendInLoop(Buffer &buf);
    void ShutdownInLoop();
    void StartIdleTimer(int sec);
    void StopIdleTimer();

public:
    Connection(EventLoop *loop, uint64_t conn_id, int sockfd);
    ~Connection();
    int Fd() { return _socket.Fd(); }
    uint64_t Id() { return _conn_id; }
    bool Connected() { return (_statu == CONNECTED); }
    bool Dead() { return (_statu == DISCONNECTED || _statu == DISCONNECTING); }
    void SetConnectedCallback(const ConnectedCallback &cb) { _connected_callback = cb; }
    void SetMessageCallback(const MessageCallback &cb) { _message_callback = cb; }
    void SetClosedCallback(const ClosedCallback &cb) { _closed_callback = cb; }
    void SetAnyEventCallback(const AnyEventCallback &cb) { _event_callback = cb; }
    void SetSrvClosedCallback(const ClosedCallback &cb) { _server_closed_callback = cb; }
    // 就绪：挂读监控+置 CONNECTED+调 connected 回调（投递到属主 loop）
    void Established();
    // 提交发送：拷入 _out_buffer 并挂写监控（不直接碰 socket）
    void Send(const char *data, size_t len);
    // 关闭申请：先冲待发与残留读，再走 Release
    void Shutdown();
    void Release();
    // 空闲 sec 秒无事件即释放；期间任何事件都刷新计时
    void SetIdleTimeout(int sec);
    void CancelIdleTimeout();
};

} 
