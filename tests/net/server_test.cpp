#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <future>
#include <string>
#include <thread>
#include <vector>
#include "net/server/tcp_server.hpp"

using namespace nexus;
using namespace std::chrono_literals;

// 全流程：io=2+biz=2 形态，3 条真客户端连接 echo 往返，Shutdown 后指标精确对账
TEST(Server, EchoFullFlowAndMetrics)
{
    const int PORT = 19731;
    TcpServer server(PORT);
    server.SetThreadCount(2);
    server.SetBizThreadCount(2);

    std::atomic<int> echoes{0};
    std::promise<void> all_echoed;
    server.SetMessageCallback(
        [&](const PtrConnection &conn, std::string data)
        {
            conn->Send(data.c_str(), data.size());
            if (echoes.fetch_add(1) + 1 == 3) all_echoed.set_value();
        });

    // 监听在构造时就绪，accept 派发要等 baseloop 跑起来：
    // 客户端先连只会躺在 backlog 里，时序安全
    std::thread th([&]
                   { server.Start(); });

    for (int i = 1; i <= 3; i++)
    {
        Socket cli;
        ASSERT_TRUE(cli.Connect(PORT, "127.0.0.1"));
        std::string msg = "ping-" + std::to_string(i);
        ASSERT_EQ(cli.Send(msg.c_str(), msg.size()), (ssize_t)msg.size());
        char buf[64] = {0};
        ssize_t ret = cli.Recv(buf, sizeof(buf) - 1);
        ASSERT_GT(ret, 0) << "第 " << i << " 条连接没等到 echo";
        EXPECT_EQ(std::string(buf, ret), msg);
    } // cli 析构关 fd：服务端走 EOF->Release

    ASSERT_EQ(all_echoed.get_future().wait_for(5s), std::future_status::ready)
        << "echoes=" << echoes.load();

    server.RunAfter([&]
                    { server.Shutdown(); }, 1); // 1s 后在 baseloop 线程里优雅退出
    th.join();

    auto m = server.TotalMetrics();
    EXPECT_EQ(m.total_connections, 3u);
    EXPECT_EQ(m.active_connections, 0u) << "Shutdown 后不该有活跃连接残留";
    EXPECT_EQ(m.total_bytes_read, 18u);
    EXPECT_EQ(m.total_bytes_written, 18u);
    EXPECT_GE(m.total_requests, 3);
}

// 同 conn_id 哈希进同一队列、单消费者：执行序=入队序，且全程同一条 worker
TEST(BizPool, SameConnTasksSerialOrdered)
{
    BizThreadPool pool;
    pool.SetThreadCount(2);
    pool.SetQueueSize(1000);
    pool.Create();

    const int N = 100;
    std::vector<int> seq;
    std::vector<std::thread::id> tids;
    std::promise<void> done;
    auto f = done.get_future();

    for (int i = 0; i < N; i++)
        ASSERT_TRUE(pool.Post(7, [&, i]
                              {
                                  seq.push_back(i);
                                  tids.push_back(std::this_thread::get_id());
                                  if (i == N - 1) done.set_value();
                              }));

    ASSERT_EQ(f.wait_for(5s), std::future_status::ready);
    for (int i = 0; i < N; i++)
        EXPECT_EQ(seq[i], i);
    for (auto &tid : tids)
        EXPECT_EQ(tid, tids[0]) << "同连接任务被多线程跑过：哈希路由失守";
    EXPECT_EQ(pool.Dropped(), 0u);
}

// 队列满 FAST_FAIL：worker 被 gate 卡死后恰好收 4 丢 96，Post 不阻塞生产者
TEST(BizPool, QueueFullFastFail)
{
    BizThreadPool pool;
    pool.SetThreadCount(1);
    pool.SetQueueSize(4);
    pool.Create();

    std::promise<void> gate_running, gate_release;
    auto rf = gate_release.get_future();
    ASSERT_TRUE(pool.Post(0, [&]
                          {
                              gate_running.set_value(); // 确认 worker 已取走 gate 任务
                              rf.wait();                // 卡住它：队列只进不出
                          }));
    ASSERT_EQ(gate_running.get_future().wait_for(5s), std::future_status::ready);

    std::atomic<int> ran{0};
    int accepted = 0, dropped = 0;
    for (int i = 0; i < 100; i++)
        (pool.Post(0, [&]
                   { ran.fetch_add(1, std::memory_order_relaxed); })
             ? accepted++
             : dropped++);

    EXPECT_EQ(accepted, 4);
    EXPECT_EQ(dropped, 96);
    EXPECT_EQ(pool.Dropped(), (uint64_t)dropped);

    gate_release.set_value();
    for (int i = 0; i < 100 && ran.load() < 4; i++)
        std::this_thread::sleep_for(20ms);
    EXPECT_EQ(ran.load(), 4) << "已入队的任务该全部执行完";
}
