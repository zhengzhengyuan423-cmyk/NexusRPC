#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <future>
#include <thread>
#include <vector>
#include "net/event/event_loop.hpp"
#include "net/server/loop_thread.hpp"

using namespace nexus;
using namespace std::chrono_literals;

// LoopThread 起 loop：异线程 RunInLoop 的任务被真实执行——证明 loop 在跑且跨线程投递通
TEST(EventLoop, RunInLoopCrossThread)
{
    LoopThread t;
    EventLoop *loop = t.GetLoop();
    ASSERT_NE(loop, nullptr);
    EXPECT_FALSE(loop->IsInLoop()); // loop 属于 worker 线程，不是本测试线程

    std::promise<void> ran;
    auto f = ran.get_future();
    loop->RunInLoop([&]()
                    { ran.set_value(); });
    ASSERT_EQ(f.wait_for(2s), std::future_status::ready);
}

// loop 线程内再 RunInLoop：同线程就地执行——内层任务先于外层返回
TEST(EventLoop, RunInLoopSameThreadInline)
{
    LoopThread t;
    EventLoop *loop = t.GetLoop();
    std::vector<int> seq;
    std::promise<void> done;
    auto f = done.get_future();

    loop->RunInLoop(
        [&, loop]
        { // 本任务已在 loop 线程：嵌套调用必须就地跑
            seq.push_back(1);
            loop->RunInLoop([&]()
                            { seq.push_back(2); });
            seq.push_back(3);
            done.set_value();
        });
    ASSERT_EQ(f.wait_for(2s), std::future_status::ready);
    ASSERT_EQ(seq, (std::vector<int>{1, 2, 3}));
}

// QueueInLoop 永远后置：即便在 loop 线程内也不就地执行，排到当轮任务池之后
TEST(EventLoop, QueueInLoopAlwaysDeferred)
{
    LoopThread t;
    EventLoop *loop = t.GetLoop();
    std::vector<int> seq;
    std::promise<void> done;
    auto f = done.get_future();

    loop->RunInLoop(
        [&, loop]
        {
            seq.push_back(1);
            loop->QueueInLoop([&]()
                              { seq.push_back(2); done.set_value(); });
            seq.push_back(3); // 外层跑完才轮到 2
        });
    ASSERT_EQ(f.wait_for(2s), std::future_status::ready);
    ASSERT_EQ(seq, (std::vector<int>{1, 3, 2}));
}

// 异线程连投 1000 个任务：单消费者+swap 交接，执行序=入队序
TEST(EventLoop, CrossThreadTasksKeepFifo)
{
    LoopThread t;
    EventLoop *loop = t.GetLoop();
    std::atomic<int> counter{0};
    std::promise<void> done;
    auto f = done.get_future();

    for (int i = 0; i < 1000; i++)
    {
        loop->RunInLoop(
            [&, i]
            {
                EXPECT_EQ(counter.fetch_add(1), i); // 乱一个都抓
                if (i == 999) done.set_value();
            });
    }
    ASSERT_EQ(f.wait_for(5s), std::future_status::ready) << "hits=" << counter.load();
    EXPECT_EQ(counter.load(), 1000);
}

// 定时触发 + 取消：delay=1 的到期跑，取消过的不跑（真实时间等待）
TEST(EventLoop, TimerFireAndCancel)
{
    LoopThread t;
    EventLoop *loop = t.GetLoop();
    std::atomic<int> fired1{0}, fired2{0};
    loop->TimerAdd(1, 1, [&]()
                   { fired1++; });
    loop->TimerAdd(2, 1, [&]()
                   { fired2++; });
    loop->TimerCancel(2); // 投递在后：同队列 FIFO，取消赶得上（delay 还有 1s）

    std::this_thread::sleep_for(2500ms);
    EXPECT_EQ(fired1.load(), 1);
    EXPECT_EQ(fired2.load(), 0);
}

// 刷新延期：refresh=多挂一份引用，对象只在最后一份被 clear 时析构触发。
// delay=2+持续 refresh：停刷后至少再等一个 delay 才到期
TEST(EventLoop, TimerRefreshExtendsDeadline)
{
    LoopThread t;
    EventLoop *loop = t.GetLoop();
    std::atomic<int> fired{0};
    std::atomic<int64_t> fire_at{-1};
    loop->TimerAdd(9, 2, [&]()
                   {
                       fired++;
                       fire_at = std::chrono::duration_cast<std::chrono::milliseconds>(
                                     std::chrono::steady_clock::now().time_since_epoch())
                                     .count();
                   });

    auto t0 = std::chrono::steady_clock::now();
    auto ms = [&](std::chrono::steady_clock::time_point tp)
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(tp - t0).count();
    };
    // 0.3s 一次刷 6 次（共 1.8s），每次刷新重新延 1s：中途绝不该到期
    for (int i = 0; i < 6; i++)
    {
        std::this_thread::sleep_for(300ms);
        loop->TimerRefresh(9);
    }
    int64_t last_refresh = ms(std::chrono::steady_clock::now());
    EXPECT_EQ(fired.load(), 0) << "refresh 期间就触发了";

    for (int i = 0; i < 30 && fired.load() == 0; i++)
        std::this_thread::sleep_for(100ms);
    ASSERT_EQ(fired.load(), 1);
    int64_t got = fire_at.load();
    EXPECT_GE(got - last_refresh, 900) << "触发时刻应落在最后一次 refresh 之后约 1s";
}
