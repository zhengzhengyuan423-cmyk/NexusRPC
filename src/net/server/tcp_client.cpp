#include "net/server/tcp_client.hpp"
#include "net/conn/socket.hpp"

namespace nexus {

TcpClient::TcpClient(const std::string &ip, uint16_t port) : _ip(ip), _port(port) {}

TcpClient::~TcpClient() { Shutdown(); }

bool TcpClient::Connect()
{
    Socket sock;
    if (!sock.Create())
        return false;
    if (!sock.Connect(_port, _ip))
        return false;
    int fd = sock.Fd();
    sock.Detach(); // fd 所有权移交给 Connection

    EventLoop *loop = _loop_thread.GetLoop();
    _conn.reset(new Connection(loop, 1, fd));
    _conn->SetConnectedCallback(_connected_callback);
    _conn->SetClosedCallback(_closed_callback);
    // Buffer* 在 io loop 里拷成 string 才出门（与服务端同一 wrapping）
    _conn->SetMessageCallback([this](const PtrConnection &c, Buffer *buf)
                              {
                                  std::string data = buf->ReadStringAndPop(buf->ReadableSize());
                                  if (_message_callback)
                                      _message_callback(c, data);
                              });
    _conn->Established();
    return true;
}

void TcpClient::Shutdown()
{
    if (!_conn)
        return;
    EventLoop *loop = _loop_thread.GetLoop();
    loop->RunInLoop([c = _conn]()
                    { c->Shutdown(); });
    // join 在本方法里做完：排空跑的 ReleaseInLoop 会回调 ClosedCallback，
    // 那些回调的宿主此刻必须还活着
    _loop_thread.Join();
    _conn.reset();
}

}
