// INet 门面的当前实现：包装 TcpServer/TcpClient。
// 换实现（asio/io_uring）= 新写一对派生类，本文件与 net/api/inet.hpp 之外谁都不许知道 epoll 存在。
#include "net/api/inet.hpp"
#include "net/server/tcp_client.hpp"
#include "net/server/tcp_server.hpp"
#include <mutex>
#include <unordered_map>

namespace nexus {

// ———— NetServer ——————————————————————————————————————————————
class NetServer::Impl
{
public:
    Impl(uint16_t port, const NetOptions &opts)
    {
        _server.reset(new TcpServer(port));
        _server->SetThreadCount(opts.io_threads);
        _server->SetBizThreadCount(opts.biz_threads);
        if (opts.idle_release_sec > 0)
            _server->SetIdleTimeout(opts.idle_release_sec);
        _server->SetConnectedCallback([this](const PtrConnection &c)
                                      {
                                          {
                                              std::lock_guard<std::mutex> lk(_mutex);
                                              _conns.emplace(c->Id(), c);
                                          }
                                          if (_on_open) _on_open(c->Id());
                                      });
        _server->SetMessageCallback([this](const PtrConnection &c, std::string data)
                                    {
                                        if (_on_msg) _on_msg(c->Id(), data);
                                    });
        _server->SetClosedCallback([this](const PtrConnection &c)
                                   {
                                       uint64_t id = c->Id();
                                       // C1：close 排在该连接全部已投递任务之后
                                       _server->PostConnCleanup(id, [this, id]
                                                                {
                                                                    {
                                                                        std::lock_guard<std::mutex> lk(_mutex);
                                                                        _conns.erase(id);
                                                                    }
                                                                    if (_on_close) _on_close(id);
                                                                });
                                   });
    }

    PtrConnection Find(uint64_t conn_id)
    {
        std::lock_guard<std::mutex> lk(_mutex);
        auto it = _conns.find(conn_id);
        return it == _conns.end() ? nullptr : it->second;
    }

    std::mutex _mutex; // 只护 _conns 查/插/擦，不跨回调持有
    std::unordered_map<uint64_t, PtrConnection> _conns;
    INet::BytesFn _on_msg;
    INet::CloseFn _on_close;
    INet::ConnFn _on_open;
    // _server 最后声明→最先销毁：它析构排空 loop 时，上面存的回调函数对象还活着
    std::unique_ptr<TcpServer> _server;
};

NetServer::NetServer(uint16_t port, NetOptions opts) : _impl(new Impl(port, opts)) {}
NetServer::~NetServer() = default;

void NetServer::SetOnMessage(BytesFn fn) { _impl->_on_msg = std::move(fn); }
void NetServer::SetOnClose(CloseFn fn) { _impl->_on_close = std::move(fn); }
void NetServer::SetOnConnect(ConnFn fn) { _impl->_on_open = std::move(fn); }

bool NetServer::Send(uint64_t conn_id, const std::string &data)
{
    auto c = _impl->Find(conn_id);
    if (!c)
        return false; // C3：句柄失效
    c->Send(data.data(), data.size());
    return true;
}

void NetServer::Close(uint64_t conn_id)
{
    if (auto c = _impl->Find(conn_id))
        c->Shutdown();
}

void NetServer::After(uint64_t timer_id, int sec, TimerFn cb)
{
    // 覆盖语义：先撤旧槽再排新任务（同一 baseloop，投递 FIFO 保先后）
    _impl->_server->TimerCancel(timer_id);
    _impl->_server->TimerAdd(timer_id, sec, std::move(cb));
}

void NetServer::CancelTimer(uint64_t timer_id) { _impl->_server->TimerCancel(timer_id); }

void NetServer::Start() { _impl->_server->Start(); }

void NetServer::Stop()
{
    TcpServer *srv = _impl->_server.get();
    srv->RunAfter([srv] { srv->Shutdown(); }, 1);
}

// ———— NetClient ———————————————————————————————————————————————
class NetClient::Impl
{
public:
    static constexpr uint64_t kConnId = 1; // 客户端独苗连接，id 恒定

    Impl(const std::string &ip, uint16_t port) : _ip(ip), _port(port) {}

    // TcpClient 的 loop 线程一次性，Stop 后不可复用——重连=重建内核
    void EnsureTcp()
    {
        if (_tcp)
            return;
        _tcp.reset(new TcpClient(_ip, _port));
        _tcp->SetConnectedCallback([this](const PtrConnection &)
                                   {
                                       if (_on_open) _on_open(kConnId);
                                   });
        _tcp->SetMessageCallback([this](const PtrConnection &, std::string data)
                                 {
                                     if (_on_msg) _on_msg(kConnId, data);
                                 });
        _tcp->SetClosedCallback([this](const PtrConnection &)
                                {
                                    if (_on_close) _on_close(kConnId);
                                });
    }

    std::string _ip;
    uint16_t _port;
    INet::BytesFn _on_msg;
    INet::CloseFn _on_close;
    INet::ConnFn _on_open;
    // 同上：_tcp 析构会发 FIN→排空→join，排空期间回调够得着上面三位
    std::unique_ptr<TcpClient> _tcp;
};

NetClient::NetClient(const std::string &ip, uint16_t port) : _impl(new Impl(ip, port)) {}
NetClient::~NetClient() = default;

void NetClient::SetOnMessage(BytesFn fn) { _impl->_on_msg = std::move(fn); }
void NetClient::SetOnClose(CloseFn fn) { _impl->_on_close = std::move(fn); }
void NetClient::SetOnConnect(ConnFn fn) { _impl->_on_open = std::move(fn); }

bool NetClient::Connect()
{
    _impl->EnsureTcp();
    return _impl->_tcp->Connect();
}
uint64_t NetClient::Id() const { return Impl::kConnId; }

bool NetClient::Send(uint64_t conn_id, const std::string &data)
{
    if (conn_id != Id() || !_impl->_tcp)
        return false;
    auto c = _impl->_tcp->GetConn();
    // 被动挂断（FIN/EOF）不会 reset _conn：只查非空会把包塞进尸块并谎报成功
    if (!c || c->Dead())
        return false;
    c->Send(data.data(), data.size());
    return true;
}

void NetClient::Close(uint64_t conn_id)
{
    if (conn_id != Id() || !_impl->_tcp)
        return;
    if (auto c = _impl->_tcp->GetConn())
        c->Shutdown();
}

void NetClient::After(uint64_t timer_id, int sec, TimerFn cb)
{
    if (!_impl->_tcp)
        return;
    EventLoop *loop = _impl->_tcp->GetLoop();
    loop->TimerCancel(timer_id);
    loop->TimerAdd(timer_id, sec, std::move(cb));
}

void NetClient::CancelTimer(uint64_t timer_id)
{
    if (_impl->_tcp)
        _impl->_tcp->GetLoop()->TimerCancel(timer_id);
}

void NetClient::Stop()
{
    if (!_impl->_tcp)
        return;
    _impl->_tcp->Shutdown();
    _impl->_tcp.reset(); // 拆掉内核，Connect 再入即重建
}

}
