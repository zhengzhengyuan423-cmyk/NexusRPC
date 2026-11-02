#include "net/conn/connection.hpp"

namespace nexus {

Connection::Connection(EventLoop *loop, uint64_t conn_id, int sockfd)
    : _conn_id(conn_id), _idle_timer(false), _loop(loop), _statu(CONNECTING),
      _socket(sockfd), _channel(loop, sockfd)
{
    _channel.SetCloseCallback(std::bind(&Connection::HandleClose, this));
    _channel.SetEventCallback(std::bind(&Connection::HandleEvent, this));
    _channel.SetReadCallback(std::bind(&Connection::HandleRead, this));
    _channel.SetWriteCallback(std::bind(&Connection::HandleWrite, this));
    _channel.SetErrorCallback(std::bind(&Connection::HandleError, this));
}

Connection::~Connection() { DLOG << "RELEASE CONNECTION:" << this; }

void Connection::HandleRead()
{
    char buf[65536];
    ssize_t ret = _socket.TryRecv(buf, 65535);
    if (ret == 0)
    {
        return HandleClose(); // 对端关闭
    }
    if (ret < 0)
    {
        return; // EAGAIN/EINTR：暂时无数据
    }
    _in_buffer.WriteAndPush(buf, ret);
    _loop->GetMetrics().OnBytesRead(ret);
    if (_in_buffer.ReadableSize() > 0)
    {
        _loop->GetMetrics().OnRequest();
        // 未设回调=吞掉，不炸 loop
        if (_message_callback)
            _message_callback(shared_from_this(), &_in_buffer);
        return;
    }
}

void Connection::HandleWrite()
{
    ssize_t ret = _socket.TrySend(_out_buffer.ReadPosition(), _out_buffer.ReadableSize());
    if (ret < 0)
    {
        return; // 内核发送缓冲区满：保持写监控，下次可写再冲
    }
    _loop->GetMetrics().OnBytesWritten(ret);
    _out_buffer.MoveReadOffset(ret);
    if (_out_buffer.ReadableSize() == 0)
    {
        _channel.DisableWrite();
        if (_statu == DISCONNECTING)
        {
            return Release(); // 待发数据冲完，兑现关闭
        }
    }
    return;
}

void Connection::HandleClose()
{
    // 挂断前先把手里的残留数据过一遍，不丢请求
    if (_in_buffer.ReadableSize() > 0)
    {
        if (_message_callback)
            _message_callback(shared_from_this(), &_in_buffer);
    }
    return Release();
}

void Connection::HandleEvent()
{
    if (_idle_timer == true)
    {
        _loop->TimerRefresh(_conn_id);
    }
    if (_event_callback)
    {
        _event_callback(shared_from_this());
    }
}

void Connection::EstablishedInLoop()
{
    assert(_statu == CONNECTING);
    _statu = CONNECTED;
    _channel.EnableRead();
    _loop->GetMetrics().OnConnectionCreated();
    if (_connected_callback) _connected_callback(shared_from_this());
}

void Connection::ReleaseInLoop()
{
    if (_statu == DISCONNECTED) return; // 幂等闸：重复 Release 不双关 fd/双减计数
    _statu = DISCONNECTED;
    _channel.Remove();
    _socket.Close();
    _loop->GetMetrics().OnConnectionClosed();
    if (_loop->HasTimer(_conn_id)) StopIdleTimer();
    // 先调回调再让 TcpServer 摘表：避免表先空、shared_ptr 先亡、回调现场悬空
    if (_closed_callback) _closed_callback(shared_from_this());
    if (_server_closed_callback) _server_closed_callback(shared_from_this());
}

void Connection::SendInLoop(Buffer &buf)
{
    if (_statu == DISCONNECTED) return;
    _out_buffer.WriteBufferAndPush(buf);
    if (_channel.Writable() == false)
    {
        _channel.EnableWrite();
    }
}

void Connection::ShutdownInLoop()
{
    _statu = DISCONNECTING;
    if (_in_buffer.ReadableSize() > 0)
    {
        if (_message_callback) _message_callback(shared_from_this(), &_in_buffer);
    }
    if (_out_buffer.ReadableSize() > 0)
    {
        if (_channel.Writable() == false)
        {
            _channel.EnableWrite();
        }
    }
    if (_out_buffer.ReadableSize() == 0)
    {
        Release();
    }
}

void Connection::StartIdleTimer(int sec)
{
    _idle_timer = true;
    if (_loop->HasTimer(_conn_id))
    {
        return _loop->TimerRefresh(_conn_id);
    }
    // weak_ptr 捕获：连接先亡则任务到期时 lock 失败，自动空转
    std::weak_ptr<Connection> weak_self = shared_from_this();
    _loop->TimerAdd(_conn_id, sec, [weak_self]()
                    {
                        if (auto self = weak_self.lock())
                        {
                            self->Release();
                        }
                    });
}

void Connection::StopIdleTimer()
{
    _idle_timer = false;
    if (_loop->HasTimer(_conn_id))
    {
        _loop->TimerCancel(_conn_id);
    }
}

void Connection::Established()
{
    _loop->RunInLoop(std::bind(&Connection::EstablishedInLoop, this));
}

void Connection::Send(const char *data, size_t len)
{
    // 先拷进临时 Buffer 再投递：data 可能是调用方的栈上临时空间，
    // 任务池延后执行时原空间未必还活着
    Buffer buf;
    buf.WriteAndPush(data, len);
    _loop->RunInLoop(std::bind(&Connection::SendInLoop, this, std::move(buf)));
}

void Connection::Shutdown()
{
    _loop->RunInLoop(std::bind(&Connection::ShutdownInLoop, this));
}

void Connection::Release()
{
    // QueueInLoop 而非 RunInLoop：释放必须排在当前回调栈之后。
    // 任务携带强引用：排队期间对象不许被别的线程拆掉，否则排空的 ReleaseInLoop 踩到死对象
    _loop->QueueInLoop([self = shared_from_this()]() { self->ReleaseInLoop(); });
}

void Connection::SetIdleTimeout(int sec)
{
    _loop->RunInLoop(std::bind(&Connection::StartIdleTimer, this, sec));
}

void Connection::CancelIdleTimeout()
{
    _loop->RunInLoop(std::bind(&Connection::StopIdleTimer, this));
}

} 
