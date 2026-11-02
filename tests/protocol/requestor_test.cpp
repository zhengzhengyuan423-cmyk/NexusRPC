#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <mutex>
#include <thread>
#include "fake_net.hpp"
#include "net/conn/socket.hpp"
#include "net/server/tcp_server.hpp"
#include "protocol/requestor.hpp"
#include "rpc/rpc_client.hpp"
#include "rpc/rpc_server.hpp"

using namespace std::chrono;
using namespace nexus;
using namespace nexus::rpc;

static void WaitListenReady(int port)
{
    for (int i = 0; i < 100; i++)
    {
        Socket probe;
        probe.Create();
        if (probe.Connect((uint16_t)port, "127.0.0.1"))
            return;
        std::this_thread::sleep_for(milliseconds(50));
    }
    FAIL() << "server not ready";
}

static void RegisterCalculator(RpcServer &srv)
{
    srv.RegisterService("Add", {{"a", VType::INTEGRAL}, {"b", VType::INTEGRAL}},
                        VType::INTEGRAL,
                        [](const Json::Value &p, Json::Value &r)
                        { r = p["a"].asInt() + p["b"].asInt(); });
    srv.RegisterService("Sub", {{"a", VType::INTEGRAL}, {"b", VType::INTEGRAL}},
                        VType::INTEGRAL,
                        [](const Json::Value &p, Json::Value &r)
                        { r = p["a"].asInt() - p["b"].asInt(); });
}

class LiveRpc
{
public:
    LiveRpc(int port, int biz) : _srv(port, NetOptions{0, biz, 0})
    {
        RegisterCalculator(_srv);
        _th = std::thread([this] { _srv.Start(); });
        WaitListenReady(port);
    }
    ~LiveRpc()
    {
        _srv.Stop();
        _th.join(); // 契约兑现点：Start 线程返回=收摊完成
    }

private:
    RpcServer _srv;
    std::thread _th;
};

static Json::Value AB(int a, int b)
{
    Json::Value p;
    p["a"] = a;
    p["b"] = b;
    return p;
}

// SYNC/FUTURE/CALLBACK 三形态只差等待位置，配对与清算逻辑同一套
TEST(Requestor, ThreeWays)
{
    {
        LiveRpc live(19881, 2);
        RpcClient cli("127.0.0.1", 19881);
        ASSERT_TRUE(cli.Connect());
        RCode code;
        Json::Value result;
        ASSERT_TRUE(cli.Call("Add", AB(3, 4), 5, code, result));
        EXPECT_EQ(code, RCode::RCODE_OK);
        EXPECT_EQ(result.asInt(), 7);
        ASSERT_TRUE(cli.Call("Sub", AB(10, 3), 5, code, result));
        EXPECT_EQ(result.asInt(), 7); // 同一 Requestor 连发两单：rid 各配各的回执
    }
    {
        LiveRpc live(19882, 0);
        RpcClient cli("127.0.0.1", 19882);
        ASSERT_TRUE(cli.Connect());
        std::future<Message::ptr> fut;
        ASSERT_TRUE(cli.Call("Add", AB(20, 22), fut));
        ASSERT_EQ(fut.wait_for(seconds(5)), std::future_status::ready);
        Message::ptr msg = fut.get();
        ASSERT_TRUE(msg);
        auto rsp = std::dynamic_pointer_cast<RpcResponse>(msg);
        EXPECT_EQ(rsp->GetRCode(), RCode::RCODE_OK);
        EXPECT_EQ(rsp->Result().asInt(), 42);
    }
    {
        LiveRpc live(19883, 2);
        RpcClient cli("127.0.0.1", 19883);
        ASSERT_TRUE(cli.Connect());
        int got = -1;
        std::mutex m;
        std::condition_variable cv;
        ASSERT_TRUE(cli.Call("Sub", AB(9, 1),
                             [&](Message::ptr msg)
                             {
                                 std::lock_guard<std::mutex> lk(m);
                                 got = msg ? std::dynamic_pointer_cast<RpcResponse>(msg)->Result().asInt() : -99;
                                 cv.notify_one();
                             },
                             5));
        {
            std::unique_lock<std::mutex> lk(m);
            ASSERT_TRUE(cv.wait_for(lk, seconds(5), [&] { return got != -1; }));
        }
        EXPECT_EQ(got, 8);
    }
}

// 沉默服务器：收下字节但慢回话。超时先摘表返回 false；迟到回包查无此 rid 静默丢；
// 下一单换 rid 正常走完
TEST(Requestor, TimeoutDropsAndLateDiscarded)
{
    int port = 19884;
    TcpServer srv(port);
    srv.SetMessageCallback([&srv](const PtrConnection &c, std::string data)
                           {
                               Buffer b;
                               b.WriteStringAndPush(data);
                               uint32_t mt;
                               std::string rid, body;
                               if (PeekFrame(b) == FrameState::GOOD && ParseFrame(b, mt, rid, body))
                               {
                                   std::this_thread::sleep_for(milliseconds(1500)); // 永远比 1s 超时晚
                                   RpcResponse rsp;
                                   rsp.SetRCode(RCode::RCODE_OK);
                                   rsp.SetResult(99);
                                   std::string wire = Encode((uint32_t)MType::RSP_RPC, rid, rsp.Serialize());
                                   c->Send(wire.c_str(), wire.size());
                               }
                           });
    std::thread th([&] { srv.Start(); });
    WaitListenReady(port);

    RpcClient cli("127.0.0.1", (uint16_t)port);
    ASSERT_TRUE(cli.Connect());
    RCode code;
    Json::Value result;
    auto t0 = steady_clock::now();
    EXPECT_FALSE(cli.Call("Add", AB(1, 1), 1, code, result));
    auto spent = duration_cast<milliseconds>(steady_clock::now() - t0).count();
    EXPECT_GE(spent, 900);  // 没提前跑
    EXPECT_LE(spent, 3000); // 真在超时点脱身，不是挂死
    // 迟到回包此刻到达：表已摘，静默丢，不崩不二次兑现
    ASSERT_TRUE(cli.Call("Add", AB(2, 2), 5, code, result));
    EXPECT_EQ(code, RCode::RCODE_OK);
    EXPECT_EQ(result.asInt(), 99);

    srv.RunAfter([&srv] { srv.Shutdown(); }, 1); // 裸 TcpServer 的收摊路径
    th.join();
}

// 只收不发的服务器：pending 没人兑现，断线清算必须唤醒 30s 的等待者
TEST(Requestor, DisconnectWakesPendingWaiter)
{
    int port = 19886;
    TcpServer srv(port);
    std::thread th([&] { srv.Start(); });
    WaitListenReady(port);
    RpcClient cli("127.0.0.1", (uint16_t)port);
    ASSERT_TRUE(cli.Connect());
    std::atomic<bool> returned{false};
    bool outcome = true;
    std::thread caller([&]
                       {
                           RCode code;
                           Json::Value result;
                           outcome = cli.Call("Add", AB(1, 1), 30, code, result);
                           returned = true;
                       });
    std::this_thread::sleep_for(milliseconds(300));
    srv.RunAfter([&srv] { srv.Shutdown(); }, 1); // 1s 后整台服务器收摊
    for (int i = 0; i < 100 && !returned; i++)
        std::this_thread::sleep_for(milliseconds(100));
    EXPECT_TRUE(returned.load());
    EXPECT_FALSE(outcome);
    caller.join();
    th.join();
}
