#include <gtest/gtest.h>
#include <string>
#include "net/conn/buffer.hpp"

using namespace nexus;

// 压缩复用头部空间 + 超限扩容两条路径，内容完整性全程校验
TEST(Buffer, CompactionAndGrowth)
{
    Buffer b;
    std::string a(600, 'a');
    b.WriteAndPush(a.data(), a.size());
    EXPECT_EQ(b.ReadStringAndPop(500), std::string(500, 'a')); // reader=500 writer=600

    // 尾部只剩 424，写 500 触发前移压缩而不是扩容
    std::string c(500, 'c');
    b.WriteAndPush(c.data(), c.size());
    EXPECT_EQ(b.ReadableSize(), 600u);
    std::string back = b.ReadStringAndPop(600);
    EXPECT_EQ(back, std::string(100, 'a') + std::string(500, 'c'));

    // 整体空闲也不够：走扩容，读回全等
    std::string big(4000, 'z');
    b.WriteAndPush(big.data(), big.size());
    EXPECT_EQ(b.ReadStringAndPop(4000), big);
    EXPECT_EQ(b.ReadableSize(), 0u);
}


TEST(Buffer, OverReadAborts)
{
    Buffer b;
    char tmp[8];
    EXPECT_DEATH({ b.ReadAndPop(tmp, 1); }, "Assertion");
    std::string s(16, 'x');
    EXPECT_DEATH({ b.ReadStringAndPop(s.size() + 1); }, "Assertion");
}
