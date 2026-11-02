#include "net/server/tcp_server.hpp"
#include <sys/signalfd.h>
#include <unistd.h>

namespace nexus {

TcpServer::TcpServer(int port)
    : _next_id(0), _port(port), _timeout(0), _idle_release(false),
      _acceptor(&_baseloop, port), _pool(&_baseloop), _biz_threads(0)
{
    _acceptor.SetAcceptCallback(std::bind(&TcpServer::NewConnection, this, std::placeholders::_1));
    _acceptor.Listen(); // 先挂回调再启动监控
}

void TcpServer::RunAfterInLoop(const Functor &task, int delay)
{
    _next_id++;
    _baseloop.TimerAdd(_next_id, delay, task);
}

void TcpServer::NewConnection(int fd)
{
    // 拒绝时必须就地关 fd，否则泄漏
    if (_accept_filter && !_accept_filter(fd))
    {
        _baseloop.GetMetrics().OnConnectionRejected();
        ::close(fd);
        return;
    }

    _next_id++;
    PtrConnection conn(new Connection(_pool.NextLoop(), _next_id, fd));
    // 蓝本 Connection 的回调是 io loop 内的 Buffer*，对外 string 签名在这里适配：
    // 在 io loop 里把 Buffer 拷成 string，之后或就地执行业务、或投递 biz 队列
    MessageCallback user_cb = _message_callback;
    if (_biz_threads > 0)
    {
        conn->SetMessageCallback(
            [this, user_cb](const PtrConnection &c, Buffer *buf)
            {
                std::string data = buf->ReadStringAndPop(buf->ReadableSize());
                // 捕获 shared_ptr：任务排队期间连接对象不会被拆
                bool ok = _biz_pool.Post(c->Id(), [user_cb, c, data]()
                                         {
                                             if (user_cb) user_cb(c, data);
                                         });
                if (!ok)
                    WLOG << "biz queue full: drop " << data.size() << "B from conn " << c->Id();
            });
    }
    else
    {
        conn->SetMessageCallback(
            [user_cb](const PtrConnection &c, Buffer *buf)
            {
                std::string data = buf->ReadStringAndPop(buf->ReadableSize());
                if (user_cb) user_cb(c, data);
            });
    }
    conn->SetClosedCallback(_closed_callback);
    conn->SetConnectedCallback(_connected_callback);
    conn->SetAnyEventCallback(_event_callback);
    conn->SetSrvClosedCallback(std::bind(&TcpServer::RemoveConnection, this, std::placeholders::_1));
    if (_idle_release)
        conn->SetIdleTimeout(_timeout);
    conn->Established();
    _conns.insert(std::make_pair(_next_id, conn));
}

void TcpServer::RemoveConnectionInLoop(const PtrConnection &conn)
{
    uint64_t id = conn->Id();
    auto it = _conns.find(id);
    if (it != _conns.end())
    {
        _conns.erase(it);
    }
}

void TcpServer::RemoveConnection(const PtrConnection &conn)
{
    // _conns 只在 baseloop 线程读写：异端触发关闭时先投递回来
    _baseloop.RunInLoop(std::bind(&TcpServer::RemoveConnectionInLoop, this, conn));
}

void TcpServer::CatchSignal()
{
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGINT);
    sigaddset(&mask, SIGTERM);
    // 必须先屏蔽：信号不再走默认处置，改由 signalfd 作为文件事件读入 loop
    sigprocmask(SIG_BLOCK, &mask, NULL);
    int sfd = signalfd(-1, &mask, SFD_NONBLOCK | SFD_CLOEXEC);
    assert(sfd >= 0);
    _signal_channel.reset(new Channel(&_baseloop, sfd));
    _signal_channel->SetReadCallback(std::bind(&TcpServer::HandleSignal, this));
    _signal_channel->EnableRead();
}

void TcpServer::HandleSignal()
{
    struct signalfd_siginfo si;
    ssize_t ret = read(_signal_channel->Fd(), &si, sizeof(si));
    if (ret != (ssize_t)sizeof(si))
    {
        return; // EAGAIN：信号已被别的线程抢走，不该走优雅退出
    }
    ILOG << "signal " << si.ssi_signo << " received, shutting down...";
    Shutdown(); // 正在 baseloop 回调栈里，满足 Shutdown 的线程约定
}

void TcpServer::Start()
{
    _pool.Create();
    if (_biz_threads > 0)
        _biz_pool.Create();
    _baseloop.Start();
}

void TcpServer::Shutdown()
{
    if (_shutdown_done)
        return; // 重复 Stop 只生效一次
    _shutdown_done = true;
    ILOG << "Shutdown: stop accepting...";
    if (_signal_channel)
    {
        _signal_channel->DisableAll();
        _signal_channel->Remove();
    }
    _acceptor.GetChannel()->DisableAll();
    _acceptor.GetChannel()->Remove();

    ILOG << "Shutdown: closing " << _conns.size() << " connections...";
    // 拷贝一份再遍历：Release 会触发 RemoveConnection 改 _conns
    auto conns_copy = _conns;
    for (auto &[id, conn] : conns_copy)
    {
        conn->Shutdown();
    }

    // biz 先于 io 停：worker 手头任务的 Send 还要投给活着的 loop，join 等它跑完
    _biz_pool.Stop();
    _pool.Stop();
    _baseloop.Stop();
    ILOG << "Shutdown: done";
}

void TcpServer::ReportMetrics()
{
    auto m = TotalMetrics();
    ILOG << "=== Metrics ===";
    ILOG << "  [total] connections: " << m.active_connections
         << " (total: " << m.total_connections
         << ", rejected: " << m.rejected_connections << ")"
         << " requests: " << m.total_requests
         << " read: " << m.total_bytes_read << "B"
         << " written: " << m.total_bytes_written << "B";
    ILOG << "  [biz] threads: " << _biz_threads
         << " dropped requests: " << _biz_pool.Dropped();
}

LoopMetrics::Snapshot TcpServer::TotalMetrics()
{
    auto sum = _baseloop.GetMetrics().GetSnapshot();
    for (EventLoop *l : _pool.Loops())
    {
        auto s = l->GetMetrics().GetSnapshot();
        sum.active_connections += s.active_connections;
        sum.total_connections += s.total_connections;
        sum.total_requests += s.total_requests;
        sum.total_bytes_read += s.total_bytes_read;
        sum.total_bytes_written += s.total_bytes_written;
        sum.rejected_connections += s.rejected_connections;
    }
    return sum;
}

} 