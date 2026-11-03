// 用法: ./add_sub_client [ip] [port] — 三链路各打一行：同步 / future / 回调
#include <condition_variable>
#include <future>
#include <mutex>
#include "logger.hpp"
#include "rpc/rpc_client.hpp"

using namespace nexus;
using namespace nexus::rpc;

static Json::Value AB(int a, int b)
{
    Json::Value p;
    p["a"] = a;
    p["b"] = b;
    return p;
}

int main(int argc, char *argv[])
{
    std::string ip = argc > 1 ? argv[1] : "127.0.0.1";
    int port = argc > 2 ? atoi(argv[2]) : 9600;

    RpcClient cli(ip, (uint16_t)port);
    if (!cli.Connect())
    {
        ELOG << "connect failed";
        return 1;
    }

    // 1) 同步链路
    RCode code;
    Json::Value result;
    if (cli.Call("Add", AB(3, 4), code, result))
        ILOG << "[sync] Add(3,4)=" << result.asInt() << " rcode=" << (int)code;
    else
        ILOG << "[sync] Add failed";

    // 2) future 链路
    std::future<Message::ptr> fut;
    if (cli.Call("Sub", AB(10, 3), fut))
    {
        fut.wait_for(std::chrono::seconds(5));
        auto rsp = std::dynamic_pointer_cast<RpcResponse>(fut.get());
        ILOG << "[future] Sub(10,3)=" << rsp->Result().asInt() << " rcode=" << (int)rsp->GetRCode();
    }

    // 3) 回调链路
    std::mutex m;
    std::condition_variable cv;
    bool done = false;
    cli.Call("Add", AB(100, 23),
             [&](Message::ptr msg)
             {
                 std::lock_guard<std::mutex> lk(m);
                 if (msg)
                 {
                     auto rsp = std::dynamic_pointer_cast<RpcResponse>(msg);
                     ILOG << "[callback] Add(100,23)=" << rsp->Result().asInt();
                 }
                 else
                     ILOG << "[callback] failed (disconnect)";
                 done = true;
                 cv.notify_one();
             });
    {
        std::unique_lock<std::mutex> lk(m);
        cv.wait_for(lk, std::chrono::seconds(5), [&] { return done; });
    }
    return 0;
}
