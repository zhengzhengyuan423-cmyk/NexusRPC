#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include "sentinel/circuit_breaker.hpp"
#include "sentinel/leap_array.hpp"
#include "sentinel/rate_limiter.hpp"
#include "sentinel/sentinel.hpp"

using namespace nexus;

TEST(LeapArray, ZeroLossUnderConcurrency)
{
    LeapArray la(500, 2);
    const int64_t t = 10000; // 全线程同一时钟：窗口不轮转，纯测快路径抢加
    std::vector<std::thread> ts;
    for (int i = 0; i < 8; i++)
        ts.emplace_back([&]
                        {
                            for (int k = 0; k < 20000; k++)
                                la.Add(t);
                        });
    for (auto &th : ts)
        th.join();
    EXPECT_EQ(la.Sum(t), 160000u);
}

TEST(LeapArray, LazyRotationDropsExpired)
{
    LeapArray la(500, 2);
    la.Add(100, 3);
    la.Add(600, 5);
    EXPECT_EQ(la.Sum(600), 8u); // slot1 + slot0 都还在窗口内
    la.Add(1200, 2);            // slot2 复用 slot0 的格子：第一个 Add 就地翻新，3 蒸发
    EXPECT_EQ(la.Sum(1200), 7u);
    EXPECT_EQ(la.Sum(1700), 2u); // slot2 滑出窗口，只剩它自己
}

// FAST_FAIL 契约正面：判定不计数。探测一百次，额度一格不动
TEST(RateLimiter, CanPassNeverCounts)
{
    RateLimiter lim(5);
    for (int i = 0; i < 100; i++)
        EXPECT_TRUE(lim.CanPass(1000));
    EXPECT_EQ(lim.Passed(1000), 0u);
    for (int i = 0; i < 5; i++)
        lim.RecordPass(1000);
    EXPECT_FALSE(lim.CanPass(1000)); // 真放行的 5 笔才把额度吃满
}

TEST(RateLimiter, WarmUpRamp)
{
    RateLimiter lim(6, 3);
    EXPECT_NEAR(lim.ThresholdAt(0), 2.0, 1e-9);    // 起点 1/3
    EXPECT_NEAR(lim.ThresholdAt(1500), 4.0, 1e-9); // 中点线性
    EXPECT_NEAR(lim.ThresholdAt(3000), 6.0, 1e-9); // 满额
    EXPECT_NEAR(lim.ThresholdAt(9000), 6.0, 1e-9); // 封顶不涨
}

TEST(RateLimiter, QueueWaitReserve)
{
    RateLimiter lim(10); // 均匀间隔 100ms
    EXPECT_EQ(lim.TryReserve(1000, 500), 0);  // 首笔立刻，下一槽钉在 1100
    EXPECT_EQ(lim.TryReserve(1010, 500), 90); // 第二笔报出要等的 ms
    EXPECT_EQ(lim.TryReserve(1010, 50), -1);  // 要等 190>50 预算：拒
    EXPECT_EQ(lim.Passed(1010), 2u);          // 预约即占额度
}

TEST(CircuitBreaker, MinRequestBlocksFirstBlood)
{
    CircuitBreaker cb(0.5, -1, 0.5, 4, 1000);
    cb.RecordResult(0, false, 1);
    cb.RecordResult(0, false, 1); // 100% 失败但样本不足：比例不可信不跳闸
    EXPECT_EQ(cb.CurState(0), CBState::CLOSED);
    cb.RecordResult(0, true, 1);
    cb.RecordResult(0, true, 1); // total=4 fail=2 → 0.5 达阈值，跳
    EXPECT_EQ(cb.CurState(0), CBState::OPEN);
}

TEST(CircuitBreaker, ExceptionRatioTripThenHalfOpenProbe)
{
    CircuitBreaker cb(0.5, -1, 0.5, 2, 1000);
    cb.RecordResult(0, false, 5);
    cb.RecordResult(0, true, 5); // 1/2=0.5 → OPEN
    EXPECT_FALSE(cb.Allow(999));     // 恢复期铁闸
    EXPECT_TRUE(cb.Allow(1000));     // 到期：第一人拉闸并抢到探测票
    EXPECT_FALSE(cb.Allow(1000));    // 半开只发一张票，第二人拒
    cb.RecordResult(1000, true, 5);  // 探测通过
    EXPECT_EQ(cb.CurState(1000), CBState::CLOSED);
    EXPECT_TRUE(cb.Allow(1001));
    cb.RecordResult(1001, true, 5); // 旧失败已清零：新周期单样本不足，不再跳
    EXPECT_EQ(cb.CurState(1001), CBState::CLOSED);
}

TEST(CircuitBreaker, ProbeFailReopens)
{
    CircuitBreaker cb(0.5, -1, 0.5, 2, 1000);
    cb.RecordResult(0, false, 5);
    cb.RecordResult(0, false, 5); // 2/2 → OPEN
    EXPECT_TRUE(cb.Allow(1000));
    cb.RecordResult(1000, false, 5); // 探测失败
    EXPECT_EQ(cb.CurState(1000), CBState::OPEN);
    EXPECT_FALSE(cb.Allow(1999)); // 重新计时
    EXPECT_TRUE(cb.Allow(2000));  // 再发一张票
}

TEST(CircuitBreaker, SlowRatioStrategy)
{
    CircuitBreaker cb(0.9, 100, 0.5, 4, 1000); // 异常阈值 0.9：本单全成功，异常策略不背锅
    cb.RecordResult(0, true, 150);
    cb.RecordResult(0, true, 150); // 2 笔慢调用
    cb.RecordResult(0, true, 10);
    cb.RecordResult(0, true, 10);
    EXPECT_EQ(cb.CurState(0), CBState::OPEN); // 慢比例 2/4=0.5 → 跳
}

TEST(Sentinel, UnconfiguredPassThroughAndQuota)
{
    Sentinel s;
    int64_t w = -1;
    EXPECT_TRUE(s.Entry("Del", 0, w)); // 未配置=治理关：A 组直通
    EXPECT_EQ(w, 0);
    Sentinel::LimiterCfg lmt;
    lmt.enabled = true;
    lmt.qps = 2;
    s.Configure("Add", lmt);
    ASSERT_TRUE(s.Entry("Add", 0, w));
    s.Exit("Add", true, 5, 0);
    ASSERT_TRUE(s.Entry("Add", 0, w));
    s.Exit("Add", true, 5, 0);
    EXPECT_FALSE(s.Entry("Add", 0, w)); // 额度满
    EXPECT_TRUE(s.Entry("Del", 0, w));  // 别人不受牵连
}

TEST(Sentinel, BreakerTripsThroughChain)
{
    Sentinel s;
    Sentinel::LimiterCfg lmt;
    lmt.enabled = true;
    lmt.qps = 1000; // 限流器当旁观者：全程绿灯
    Sentinel::BreakerCfg bkr;
    bkr.enabled = true;
    bkr.ex_ratio = 0.5;
    bkr.min_request = 2;
    bkr.recovery_ms = 1000;
    s.Configure("Add", lmt, bkr);
    int64_t w;
    ASSERT_TRUE(s.Entry("Add", 0, w));
    s.Exit("Add", false, 5, 0);
    ASSERT_TRUE(s.Entry("Add", 1, w));
    s.Exit("Add", false, 5, 1); // 2/2 失败 → OPEN
    EXPECT_FALSE(s.Entry("Add", 2, w));
    EXPECT_TRUE(s.Entry("Add", 1002, w)); // 半开票：票从限流之后才轮到熔断发
    s.Exit("Add", true, 5, 1002);
    EXPECT_TRUE(s.Entry("Add", 1003, w)); // 探测通过全开
}

TEST(Sentinel, QueueWaitMode)
{
    Sentinel s;
    Sentinel::LimiterCfg lmt;
    lmt.enabled = true;
    lmt.qps = 10;
    lmt.queueing_ms = 50;
    s.Configure("Add", lmt);
    int64_t w;
    ASSERT_TRUE(s.Entry("Add", 1000, w)); // 首笔占槽，wait 0
    EXPECT_EQ(w, 0);
    EXPECT_FALSE(s.Entry("Add", 1000, w)); // 第二笔要等 100>50 预算：拒
}
