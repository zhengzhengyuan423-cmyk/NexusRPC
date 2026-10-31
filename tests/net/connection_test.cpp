#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <future>
#include <string>
#include <thread>
#include <vector>
#include <sys/socket.h>
#include <unistd.h>
#include "net/conn/connection.hpp"
#include "net/server/loop_thread.hpp"

using namespace nexus;
using namespace std::chrono_literals;

// 全活流程：Established->读->echo 回写->对端 close->EOF 读净后单次 Release
TEST(Connection, EchoFlowThenPeerClose)
{
    int fds[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
    LoopThread t;
    EventLoop *loop = t.GetLoop();

    PtrConnection c(new Connection(loop, 1, fds[0]));
    std::atomic<int> closed{0};
    std::promise<void> closed_p;
    c->SetMessageCallback(
        [](const PtrConnection &cc, Buffer *buf)
        {
            std::string d = buf->ReadStringAndPop(buf->ReadableSize());
            cc->Send(d.c_str(), d.size());
        });
    c->SetClosedCallback(
        [&](const PtrConnection &)
        {
            closed++;
            closed_p.set_value();
        });
    c->Established();

    ASSERT_EQ(::write(fds[1], "hi", 2), 2);
    char rb[8] = {0};
    ASSERT_EQ(::read(fds[1], rb, 2), 2); // 客户端 fd 是阻塞的：等 echo 到手
    EXPECT_EQ(std::string(rb, 2), "hi");

    ::close(fds[1]); // 对端 close：实测带 IN|RDNORM|HUP，EOF 先派发读再 close
    ASSERT_EQ(closed_p.get_future().wait_for(3s), std::future_status::ready);
    EXPECT_EQ(closed.load(), 1) << "Release 不幂等或派发重入";
    EXPECT_FALSE(c->Connected());
}

// 256KB 大发送：内核缓冲区装不下时靠写事件续命，内容逐字节完整到达
TEST(Connection, LargeSendFlushesAll)
{
    int fds[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
    LoopThread t;
    PtrConnection c(new Connection(t.GetLoop(), 2, fds[0]));
    c->Established();

    std::string payload;
    for (int i = 0; i < 256 * 1024; i++)
        payload += char('a' + i % 26);
    c->Send(payload.data(), payload.size());

    std::vector<char> got;
    char buf[8192];
    while (got.size() < payload.size())
    {
        ssize_t n = ::read(fds[1], buf, sizeof(buf));
        ASSERT_GT(n, 0) << "客户端还没收满就断了";
        got.insert(got.end(), buf, buf + n);
    }
    EXPECT_TRUE(std::equal(payload.begin(), payload.end(), got.begin()));
    ::close(fds[1]);
}

// Shutdown 语义：先冲完待发再关——客户端收满全部数据后拿到 EOF，closed 恰好一次
TEST(Connection, ShutdownFlushesPendingThenCloses)
{
    int fds[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
    LoopThread t;
    PtrConnection c(new Connection(t.GetLoop(), 3, fds[0]));
    std::promise<void> closed_p;
    std::atomic<int> closed{0};
    c->SetClosedCallback(
        [&](const PtrConnection &)
        {
            closed++;
            closed_p.set_value();
        });
    c->Established();

    std::string payload(128 * 1024, 'x');
    c->Send(payload.data(), payload.size());
    c->Shutdown(); // 紧跟投递：队列里排在 Send 之后，待发冲完才真关

    size_t total = 0;
    char buf[8192];
    ssize_t n;
    while ((n = ::read(fds[1], buf, sizeof(buf))) > 0)
        total += n;
    EXPECT_EQ(n, 0) << "read 应以 EOF 收尾，不是错误";
    EXPECT_EQ(total, payload.size());
    ASSERT_EQ(closed_p.get_future().wait_for(3s), std::future_status::ready);
    EXPECT_EQ(closed.load(), 1);
    ::close(fds[1]);
}

// 空闲超时：有流量就一直续命，停流后定时器到点释放。
// delay=2：轮子 1s 粒度下 delay=1 的 refresh 副本与原件同格，续不活；delay>=2 续命窗口才成立
TEST(Connection, IdleTimeoutRefreshAndFire)
{
    int fds[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
    LoopThread t;
    PtrConnection c(new Connection(t.GetLoop(), 4, fds[0]));
    std::promise<void> closed_p;
    c->SetMessageCallback([](const PtrConnection &, Buffer *buf)
                          { buf->ReadStringAndPop(buf->ReadableSize()); });
    c->SetClosedCallback([&](const PtrConnection &)
                         { closed_p.set_value(); });
    c->Established();
    c->SetIdleTimeout(2);

    auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < 6; i++)
    {
        std::this_thread::sleep_until(t0 + 300ms * (i + 1));
        ASSERT_EQ(::write(fds[1], "x", 1), 1); // 任何事件都刷定时器
    }
    auto f = closed_p.get_future();
    EXPECT_EQ(f.wait_for(300ms), std::future_status::timeout) << "有流量期间不该被踢";

    // 1.8s 停流：最后一批副本落在停流前最后一格的 delay 窗外，约 3s 兑现
    ASSERT_EQ(f.wait_for(4s), std::future_status::ready) << "停流后空闲定时器没兑现";
    EXPECT_FALSE(c->Connected());
    ::close(fds[1]);
}
