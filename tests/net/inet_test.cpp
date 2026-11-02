#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include "net/api/inet.hpp"
#include "net/conn/socket.hpp"

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

static bool ConnectRetry(NetClient &cli)
{
    for (int i = 0; i < 100; i++)
    {
        if (cli.Connect())
            return true;
        std::this_thread::sleep_for(milliseconds(50));
    }
    return false;
}

class LiveNetServer
{
public:
    LiveNetServer(int port, const NetOptions &opts) : _port(port), _srv((uint16_t)port, opts) {}
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
        FAIL() << "listener not up";
    }
    void Stop()
    {
        _srv.Stop();
        if (_th.joinable())
            _th.join(); // 契约兑现点：Start 线程返回=再无回调
    }
    NetServer &Srv() { return _srv; }
    ~LiveNetServer()
    {
        if (_th.joinable())
            Stop();
    }

private:
    int _port;
    NetServer _srv;
    std::thread _th;
};

// C1 顺序（biz=0 就地兑现 + biz=2 队列兑现）与 C3 死句柄，一次测齐
TEST(INet, Contracts)
{
    {
        NetServer srv(19901);
        std::mutex m;
        std::vector<std::string> ev;
        srv.SetOnConnect([&](uint64_t id)
                         { std::lock_guard<std::mutex> lk(m); ev.push_back("open:" + std::to_string(id)); });
        srv.SetOnMessage([&](uint64_t id, const std::string &b)
                         {
                             {
                                 std::lock_guard<std::mutex> lk(m);
                                 ev.push_back("msg:" + std::to_string(id) + ":" + b);
                             }
                             srv.Send(id, b);
                         });
        srv.SetOnClose([&](uint64_t id)
                       { std::lock_guard<std::mutex> lk(m); ev.push_back("close:" + std::to_string(id)); });
        std::thread th([&] { srv.Start(); });

        NetClient cli("127.0.0.1", 19901);
        std::string echo;
        cli.SetOnMessage([&](uint64_t, const std::string &b)
                         { std::lock_guard<std::mutex> lk(m); echo = b; });
        ASSERT_TRUE(ConnectRetry(cli));
        ASSERT_TRUE(cli.Send(cli.Id(), "ping"));
        EXPECT_TRUE(WaitFor(
            [&]
            {
                std::lock_guard<std::mutex> lk(m);
                return echo == "ping";
            },
            3000));
        cli.Stop(); // 发 FIN → 服务端 on_close（biz=0 就地兑现 C1）
        EXPECT_TRUE(WaitFor(
            [&]
            {
                std::lock_guard<std::mutex> lk(m);
                if (ev.size() != 3)
                    return false;
                uint64_t id = std::stoull(ev[0].substr(5));
                return ev[1] == "msg:" + std::to_string(id) + ":ping" && ev[2] == "close:" + std::to_string(id);
            },
            3000));
        srv.Stop();
        th.join();
    }
    {
        NetOptions opts{1, 2, 0};
        NetServer srv(19902, opts);
        std::mutex m;
        std::vector<std::string> ev;
        std::atomic<uint64_t> the_id{0};
        srv.SetOnConnect([&](uint64_t id)
                         { std::lock_guard<std::mutex> lk(m); ev.push_back("open:" + std::to_string(id)); });
        srv.SetOnMessage([&](uint64_t id, const std::string &b)
                         {
                             std::lock_guard<std::mutex> lk(m);
                             ev.push_back("msg:" + std::to_string(id) + ":" + b);
                             the_id = id;
                         });
        srv.SetOnClose([&](uint64_t id)
                       { std::lock_guard<std::mutex> lk(m); ev.push_back("close:" + std::to_string(id)); });
        std::thread th([&] { srv.Start(); });

        NetClient cli("127.0.0.1", 19902);
        ASSERT_TRUE(ConnectRetry(cli));
        ASSERT_TRUE(cli.Send(cli.Id(), "one"));
        EXPECT_TRUE(WaitFor([&] { return the_id.load() != 0; }, 3000));
        std::this_thread::sleep_for(milliseconds(200));
        srv.Close(the_id.load());
        // C1（biz=2）：msg 任务先进队列，close 清理排其后——三个事件严格这个顺序
        EXPECT_TRUE(WaitFor(
            [&]
            {
                std::lock_guard<std::mutex> lk(m);
                return ev.size() == 3 && ev[2] == "close:" + std::to_string(the_id.load());
            },
            3000));
        {
            std::lock_guard<std::mutex> lk(m);
            EXPECT_EQ(ev[0], "open:" + std::to_string(the_id.load()));
            EXPECT_EQ(ev[1], "msg:" + std::to_string(the_id.load()) + ":one");
        }
        EXPECT_FALSE(srv.Send(the_id.load(), "late")); // C3：死句柄 Send 返回 false
        srv.Close(the_id.load());                      // 幂等不炸
        srv.Stop();
        th.join();
    }
}

// 定时恰好一次 / Cancel 压制 / 同 id 覆盖 / 客户端侧 After——门面定时器四连
TEST(INet, TimerSemantics)
{
    LiveNetServer live(19903, {});
    live.Start();
    std::atomic<int> once{0};
    live.Srv().After(7, 1, [&] { once++; });
    std::this_thread::sleep_for(milliseconds(2200));
    EXPECT_EQ(once.load(), 1);
    std::this_thread::sleep_for(milliseconds(1200));
    EXPECT_EQ(once.load(), 1); // 一次性：到期退役后轮子不再复活它

    std::atomic<int> cancelled{0};
    live.Srv().After(8, 1, [&] { cancelled++; });
    live.Srv().CancelTimer(8);
    std::this_thread::sleep_for(milliseconds(1800));
    EXPECT_EQ(cancelled.load(), 0);

    std::atomic<int> old_n{0}, new_n{0};
    live.Srv().After(9, 2, [&] { old_n++; });
    live.Srv().After(9, 1, [&] { new_n++; }); // 同 id 覆盖：旧的作废
    std::this_thread::sleep_for(milliseconds(1400));
    EXPECT_EQ(new_n.load(), 1);
    EXPECT_EQ(old_n.load(), 0);
    std::this_thread::sleep_for(milliseconds(1200));
    EXPECT_EQ(new_n.load(), 1);
    EXPECT_EQ(old_n.load(), 0);
    live.Stop();

    LiveNetServer live2(19906, {});
    live2.Start();
    NetClient cli("127.0.0.1", 19906);
    ASSERT_TRUE(ConnectRetry(cli));
    std::atomic<int> n{0};
    cli.After(3, 1, [&] { n++; });
    std::this_thread::sleep_for(milliseconds(1800));
    EXPECT_EQ(n.load(), 1);
    cli.Stop();
    live2.Stop();
}

TEST(INet, ReconnectAfterStop)
{
    NetServer srv(19907);
    std::mutex m;
    std::string echo;
    srv.SetOnMessage([&](uint64_t id, const std::string &b)
                     { srv.Send(id, b); });
    std::thread th([&] { srv.Start(); });

    NetClient cli("127.0.0.1", 19907);
    cli.SetOnMessage([&](uint64_t, const std::string &b)
                     { std::lock_guard<std::mutex> lk(m); echo = b; });
    ASSERT_TRUE(ConnectRetry(cli));
    ASSERT_TRUE(cli.Send(cli.Id(), "first"));
    EXPECT_TRUE(WaitFor(
        [&]
        {
            std::lock_guard<std::mutex> lk(m);
            return echo == "first";
        },
        3000));
    cli.Stop();
    {
        std::lock_guard<std::mutex> lk(m);
        echo.clear();
    }
    ASSERT_TRUE(ConnectRetry(cli)); // Stop 后 Connect 再入=重连
    ASSERT_TRUE(cli.Send(cli.Id(), "second"));
    EXPECT_TRUE(WaitFor(
        [&]
        {
            std::lock_guard<std::mutex> lk(m);
            return echo == "second";
        },
        3000));
    cli.Stop();
    cli.Stop(); // 幂等
    srv.Stop();
    th.join();
}
