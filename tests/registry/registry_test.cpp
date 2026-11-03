#include <gtest/gtest.h>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <tuple>
#include <vector>
#include "net/conn/socket.hpp"
#include "registry/registry_client.hpp"
#include "registry/registry_server.hpp"

using namespace std::chrono;
using namespace nexus;

static bool WaitFor(const std::function<bool()> &pred, int ms)
{
    for (int i = 0; i < ms / 50 + 1; i++)
    {
        if (pred())
            return true;
        std::this_thread::sleep_for(milliseconds(50));
    }
    return pred();
}

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
    void Stop()
    {
        _srv.Stop();
        if (_th.joinable())
            _th.join();
    }
    ~LiveRegistry()
    {
        if (_th.joinable())
            Stop();
    }

private:
    int _port;
    RegistryServer _srv;
    std::thread _th;
};

// watcher 事件收账本：Has 按 (instance, added) 找条目
class PushRecorder
{
public:
    void Attach(RegistryClient &cli)
    {
        cli.SetOnPush([this](const std::string &svc, const std::string &inst, bool added)
                      {
                          std::lock_guard<std::mutex> lk(_m);
                          _events.emplace_back(svc, inst, added);
                          _cv.notify_all();
                      });
    }
    bool Has(const std::string &inst, bool added, int ms)
    {
        std::unique_lock<std::mutex> lk(_m);
        return _cv.wait_for(lk, milliseconds(ms),
                            [&]
                            {
                                for (auto &[svc, got, ad] : _events)
                                    if (got == inst && ad == added)
                                        return true;
                                return false;
                            });
    }

private:
    std::mutex _m;
    std::condition_variable _cv;
    std::vector<std::tuple<std::string, std::string, bool>> _events;
};

TEST(Registry, RegisterHeartbeatList)
{
    LiveRegistry reg(19871);
    reg.Start();
    RegistryClient p("127.0.0.1", 19871);
    ASSERT_TRUE(p.Connect());
    ASSERT_TRUE(p.Register("calc", "127.0.0.1:7001"));
    ASSERT_TRUE(p.Heartbeat("calc", "127.0.0.1:7001"));
    EXPECT_FALSE(p.Heartbeat("calc", "ghost:1")); // 没登记过的实例：NOT_FOUND

    RegistryClient q("127.0.0.1", 19871);
    ASSERT_TRUE(q.Connect());
    std::vector<std::string> out;
    ASSERT_TRUE(q.List("calc", out));
    ASSERT_EQ(out.size(), 1u);
    EXPECT_EQ(out[0], "127.0.0.1:7001");
}

TEST(Registry, WatchSnapshotThenLivePush)
{
    LiveRegistry reg(19872);
    reg.Start();
    RegistryClient p1("127.0.0.1", 19872);
    ASSERT_TRUE(p1.Connect());
    ASSERT_TRUE(p1.Register("calc", "A"));

    RegistryClient w("127.0.0.1", 19872);
    PushRecorder rec;
    rec.Attach(w);
    ASSERT_TRUE(w.Connect());
    ASSERT_TRUE(w.Watch("calc"));
    EXPECT_TRUE(rec.Has("A", true, 2000)); // 订阅即快照

    RegistryClient p2("127.0.0.1", 19872);
    ASSERT_TRUE(p2.Connect());
    ASSERT_TRUE(p2.Register("calc", "B"));
    EXPECT_TRUE(rec.Has("B", true, 2000)); // 活体新增立刻推

    p2.Stop(); // 断线驱逐走 on_close，不等租约
    EXPECT_TRUE(rec.Has("B", false, 2000));
    EXPECT_TRUE(WaitFor(
        [&]
        {
            std::vector<std::string> out;
            return w.List("calc", out) && out.size() == 1;
        },
        2000));
}

TEST(Registry, NonRegFrameKillsConn)
{
    LiveRegistry reg(19874);
    reg.Start();
    // 拿垃圾字节砸注册中心：坏帧→掐线→对端立刻看到 EOF
    Socket rogue;
    rogue.Create();
    ASSERT_TRUE(rogue.Connect((uint16_t)19874, "127.0.0.1"));
    std::string junk = "hello not a frame at all";
    rogue.Send(junk.c_str(), junk.size());
    char tmp[16];
    EXPECT_EQ(rogue.Recv(tmp, sizeof(tmp)), 0); // 对端已关
}
