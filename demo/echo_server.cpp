// 用法示例：nexus echo 服务器
// 编译后: ./echo_server [port] [io_threads] [biz_threads]
#include "net/server/tcp_server.hpp"

using namespace nexus;

static void OnMessage(const PtrConnection &conn, std::string data)
{
    conn->Send(data.c_str(), data.size());
}

int main(int argc, char *argv[])
{
    int port = argc > 1 ? std::atoi(argv[1]) : 9527;
    int io_threads = argc > 2 ? std::atoi(argv[2]) : 0;  // 0=单 loop
    int biz_threads = argc > 3 ? std::atoi(argv[3]) : 0; // 0=业务就地跑在 io loop

    TcpServer server(port);
    if (io_threads > 0)
        server.SetThreadCount(io_threads);
    if (biz_threads > 0)
        server.SetBizThreadCount(biz_threads);
    server.SetMessageCallback(OnMessage);
    server.SetIdleTimeout(30); // 30s 无通信自动断开
    server.CatchSignal();      // SIGINT/SIGTERM -> 线程内优雅退出
    server.Start();            // 阻塞直到 Shutdown 停掉 baseloop
    server.ReportMetrics();
    return 0;
}
