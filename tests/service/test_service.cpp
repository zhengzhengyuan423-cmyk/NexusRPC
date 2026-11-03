#include <gtest/gtest.h>
#include <chrono>
#include <thread>
#include "net/conn/socket.hpp"
#include "registry/registry_server.hpp"
#include "service/consumer.hpp"
#include "service/provider.hpp"

using namespace std::chrono;
using namespace nexus;
using namespace nexus::rpc;

namespace
{
// p1 的 Add 原样回，p2 的 Add 多 +100：肉眼分出轮询打到了谁
void AddInto(Provider &p, int bias)
{
    p.RegisterService("Add", {{"a", VType::INTEGRAL}, {"b", VType::INTEGRAL}},
                      VType::INTEGRAL,
                      [bias](const Json::Value &pp, Json::Value &r)
                      { r = pp["a"].asInt() + pp["b"].asInt() + bias; });
}

// push 到达是异步的：带截止时间的重试循环
bool CallUntil(Consumer &c, const Json::Value &p, int want, int budget_ms)
{
    RCode code;
    Json::Value r;
    for (int t = 0; t < budget_ms; t += 50)
    {
        if (c.Call("Add", p, code, r) && r.asInt() == want)
            return true;
        std::this_thread::sleep_for(milliseconds(50));
    }
    return false;
}
} // namespace

class LiveRegistry
{
public:
    LiveRegistry(int port) : _port(port), _srv(port) {}
    void Start()
    {
        _th = std::thread([this] { _srv.Start(); });
        for (int i = 0; i < 100; i++)
        {
            Socket probe;
            probe.Create();
            if (probe.Connect((uint16_t)_port, "127.0.0.1"))
                return;
            std::this_thread::sleep_for(milliseconds(50));
        }
        FAIL() << "registry not ready";
    }
    ~LiveRegistry()
    {
        _srv.Stop();
        if (_th.joinable())
            _th.join();
    }

private:
    int _port;
    RegistryServer _srv;
    std::thread _th;
};

TEST(Service, DiscoverAndCall)
{
    LiveRegistry reg(19880);
    reg.Start();
    Provider p("calc", 19881, "127.0.0.1", 19880);
    AddInto(p, 0);
    p.Start();
    Consumer c("127.0.0.1", 19880);
    ASSERT_TRUE(c.Subscribe("calc"));
    Json::Value prm;
    prm["a"] = 6;
    prm["b"] = 7;
    RCode code;
    Json::Value r;
    ASSERT_TRUE(c.Call("Add", prm, code, r));
    EXPECT_EQ(r.asInt(), 13);
    EXPECT_EQ(code, RCode::RCODE_OK);
    c.Stop();
    p.Stop();
}

TEST(Service, RoundRobinHitsBoth)
{
    LiveRegistry reg(19882);
    reg.Start();
    Provider p1("calc", 19883, "127.0.0.1", 19882);
    AddInto(p1, 0);
    p1.Start();
    Consumer c("127.0.0.1", 19882);
    ASSERT_TRUE(c.Subscribe("calc"));
    Provider p2("calc", 19884, "127.0.0.1", 19882);
    AddInto(p2, 100);
    p2.Start(); // 上线即 push 进 consumer 名单
    Json::Value prm;
    prm["a"] = 6;
    prm["b"] = 7;
    ASSERT_TRUE(CallUntil(c, prm, 113, 3000)); // 绕到 p2
    RCode code;
    Json::Value r;
    ASSERT_TRUE(c.Call("Add", prm, code, r)); // 继续轮转回 p1
    EXPECT_EQ(r.asInt(), 13);
    c.Stop();
    p2.Stop();
    p1.Stop();
}

TEST(Service, DeadProviderEvicted)
{
    LiveRegistry reg(19885);
    reg.Start();
    Provider p1("calc", 19886, "127.0.0.1", 19885);
    AddInto(p1, 0);
    p1.Start();
    Provider p2("calc", 19887, "127.0.0.1", 19885);
    AddInto(p2, 100);
    p2.Start();
    Consumer c("127.0.0.1", 19885);
    ASSERT_TRUE(c.Subscribe("calc"));
    Json::Value prm;
    prm["a"] = 6;
    prm["b"] = 7;
    ASSERT_TRUE(CallUntil(c, prm, 113, 3000)); // 先确保打到过 p2
    p2.Stop();                                  // 断线驱逐+push
    ASSERT_TRUE(CallUntil(c, prm, 13, 3000));  // 幸存者继续供能
    RCode code;
    Json::Value r;
    // 撞死人要付账：池里摘牌、重试新连失败、push 摘名——容忍过渡期失败，
    // 但只许死在 p1 上；40 笔内必须出现 5 连中
    int streak = 0;
    for (int t = 0; t < 40 && streak < 5; t++)
    {
        if (c.Call("Add", prm, code, r))
        {
            EXPECT_EQ(r.asInt(), 13);
            streak++;
        }
        else
        {
            streak = 0;
            std::this_thread::sleep_for(milliseconds(50)); // 给摘名 push 留到件时间
        }
    }
    EXPECT_EQ(streak, 5);
    c.Stop();
    p1.Stop();
}
