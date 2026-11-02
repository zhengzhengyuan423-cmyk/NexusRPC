// 用法: ./add_sub_server [port] [io_threads] [biz_threads]
#include "logger.hpp"
#include "rpc/rpc_server.hpp"

using namespace nexus;
using namespace nexus::rpc;

int main(int argc, char *argv[])
{
    int port = argc > 1 ? atoi(argv[1]) : 9600;
    int io = argc > 2 ? atoi(argv[2]) : 1;
    int biz = argc > 3 ? atoi(argv[3]) : 2;

    RpcServer srv(port, NetOptions{io, biz, 0});
    srv.RegisterService("Add", {{"a", VType::INTEGRAL}, {"b", VType::INTEGRAL}},
                        VType::INTEGRAL,
                        [](const Json::Value &p, Json::Value &r)
                        { r = p["a"].asInt() + p["b"].asInt(); });
    srv.RegisterService("Sub", {{"a", VType::INTEGRAL}, {"b", VType::INTEGRAL}},
                        VType::INTEGRAL,
                        [](const Json::Value &p, Json::Value &r)
                        { r = p["a"].asInt() - p["b"].asInt(); });
    ILOG << "add_sub_server on " << port << " io=" << io << " biz=" << biz;
    srv.Start(); // 阻塞直至关停
    return 0;
}
