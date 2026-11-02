#include <gtest/gtest.h>
#include <arpa/inet.h>
#include "fake_net.hpp"
#include "protocol/dispatcher.hpp"
#include "protocol/message.hpp"
#include "protocol/protocol.hpp"

using namespace nexus;

static void Feed(Buffer &b, const std::string &s) { b.WriteStringAndPush(s); }

TEST(Protocol, RoundTrip)
{
    Buffer b;
    Feed(b, Encode(7, "rid-1", "hello"));
    ASSERT_EQ(PeekFrame(b), FrameState::GOOD);

    uint32_t mtype;
    std::string id, body;
    ASSERT_TRUE(ParseFrame(b, mtype, id, body));
    EXPECT_EQ(mtype, 7u);
    EXPECT_EQ(id, "rid-1");
    EXPECT_EQ(body, "hello");
    EXPECT_EQ(b.ReadableSize(), 0u);
}

// 半包 / 粘包 / 长度头被劈——三种切片姿势都必须收敛到同一帧序列
TEST(Protocol, StreamSlicing)
{
    std::string f = Encode(1, "a", "payload-bytes");
    Buffer b;
    Feed(b, f.substr(0, f.size() - 1)); // 差最后一个字节
    EXPECT_EQ(PeekFrame(b), FrameState::NEED_MORE);
    EXPECT_EQ(b.ReadableSize(), f.size() - 1); // 判定不消费

    uint32_t mtype;
    std::string id, body;
    Feed(b, f.substr(f.size() - 1));
    EXPECT_EQ(PeekFrame(b), FrameState::GOOD);
    ASSERT_TRUE(ParseFrame(b, mtype, id, body));
    EXPECT_EQ(body, "payload-bytes");

    Buffer c; // 一次喂两帧：连解两次
    Feed(c, Encode(1, "x", "AAA") + Encode(2, "yy", "BBBB"));
    ASSERT_TRUE(ParseFrame(c, mtype, id, body));
    EXPECT_EQ(mtype, 1u);
    EXPECT_EQ(id, "x");
    EXPECT_EQ(body, "AAA");
    ASSERT_TRUE(ParseFrame(c, mtype, id, body));
    EXPECT_EQ(mtype, 2u);
    EXPECT_EQ(body, "BBBB");
    EXPECT_EQ(PeekFrame(c), FrameState::NEED_MORE); // 解空后回到等包态

    Buffer d; // 长度头被劈成 2+ 的碎片
    std::string e = Encode(9, "id", "body");
    Feed(d, e.substr(0, 2));
    EXPECT_EQ(PeekFrame(d), FrameState::NEED_MORE);
    Feed(d, e.substr(2));
    EXPECT_EQ(PeekFrame(d), FrameState::GOOD);
    ASSERT_TRUE(ParseFrame(d, mtype, id, body));
    EXPECT_EQ(mtype, 9u);
}

// 天文长度/负长度/idlen 越界：当场拒，不等攒包，拒收也不消费
TEST(Protocol, BadLengthRejected)
{
    Buffer b;
    int32_t neg = htonl(-1);
    Feed(b, std::string((char *)&neg, 4) + "garbage-data");
    EXPECT_EQ(PeekFrame(b), FrameState::BAD);
    b.Clear();

    int32_t tiny = htonl(7); // total_len 连固定头 8 都凑不齐
    Feed(b, std::string((char *)&tiny, 4) + "01234567");
    EXPECT_EQ(PeekFrame(b), FrameState::BAD);
    b.Clear();

    int32_t huge = htonl(kMaxFrameSize + 1);
    Feed(b, std::string((char *)&huge, 4));
    EXPECT_EQ(PeekFrame(b), FrameState::BAD);

    int32_t idlen = 2000; // > kMaxIdSize
    int32_t total = htonl(8 + idlen);
    int32_t mt = htonl(1);
    int32_t nidlen = htonl(idlen);
    Buffer c;
    Feed(c, std::string((char *)&total, 4) + std::string((char *)&mt, 4) +
              std::string((char *)&nidlen, 4) + std::string(idlen, 'i'));
    EXPECT_EQ(PeekFrame(c), FrameState::BAD);
    EXPECT_EQ(c.ReadableSize(), 12u + idlen);

    Buffer d; // idlen 合法但 total 装不下它：body_len 为负
    int32_t bad_total = htonl(8 + 4);
    int32_t big_idlen = htonl(64);
    Feed(d, std::string((char *)&bad_total, 4) + std::string((char *)&mt, 4) +
              std::string((char *)&big_idlen, 4) + std::string(64, 'z'));
    EXPECT_EQ(PeekFrame(d), FrameState::BAD);
}

// 帧字节 → ParseFrame → Factory → Unserialize → Check → 字段还原：
// 请求与响应各走一遍完整旅程
TEST(Message, FullChainRoundTrip)
{
    RpcRequest req;
    req.SetRid("rid-42");
    req.SetMethod("Add");
    Json::Value p;
    p["a"] = 3;
    p["b"] = 4;
    req.SetParams(p);

    Buffer b;
    b.WriteStringAndPush(Encode((uint32_t)MType::REQ_RPC, req.Rid(), req.Serialize()));
    ASSERT_EQ(PeekFrame(b), FrameState::GOOD);
    uint32_t mtype;
    std::string id, body;
    ASSERT_TRUE(ParseFrame(b, mtype, id, body));

    Message::ptr msg = MessageFactory::Create(mtype);
    ASSERT_NE(msg, nullptr);
    msg->SetRid(id);
    msg->SetType((MType)mtype);
    ASSERT_TRUE(msg->Unserialize(body));
    ASSERT_TRUE(msg->Check());
    auto parsed_req = std::dynamic_pointer_cast<RpcRequest>(msg);
    ASSERT_NE(parsed_req, nullptr);
    EXPECT_EQ(parsed_req->Method(), "Add");
    EXPECT_EQ(parsed_req->Params()["a"].asInt(), 3);

    RpcResponse rsp;
    rsp.SetRCode(RCode::RCODE_OK);
    rsp.SetResult(7);
    Buffer c;
    c.WriteStringAndPush(Encode((uint32_t)MType::RSP_RPC, "rid-42", rsp.Serialize()));
    ASSERT_TRUE(ParseFrame(c, mtype, id, body));
    Message::ptr msg2 = MessageFactory::Create(mtype);
    ASSERT_NE(msg2, nullptr);
    ASSERT_TRUE(msg2->Unserialize(body));
    ASSERT_TRUE(msg2->Check());
    auto parsed_rsp = std::dynamic_pointer_cast<RpcResponse>(msg2);
    EXPECT_EQ(parsed_rsp->GetRCode(), RCode::RCODE_OK);
    EXPECT_EQ(parsed_rsp->Result().asInt(), 7);
}

// JSON 语法合法但语义非法：Unserialize 过，Check 必须拦
TEST(Message, CheckRejectsBadBody)
{
    RpcRequest r1;
    EXPECT_FALSE(r1.Check()); // 空请求：没 method 没 params
    RpcRequest r2;
    ASSERT_TRUE(r2.Unserialize("{\"method\":123,\"parameters\":{}}"));
    EXPECT_FALSE(r2.Check()); // method 不是字符串

    RpcResponse r3;
    ASSERT_TRUE(r3.Unserialize("{\"rcode\":\"oops\"}"));
    EXPECT_FALSE(r3.Check()); // rcode 非整型
    RpcResponse r4;
    ASSERT_TRUE(r4.Unserialize("{\"rcode\":0}"));
    EXPECT_FALSE(r4.Check()); // 缺 result
}

// typed 路由 / 回话只经门面 / 未登记 mtype 只报告不掐线
TEST(Dispatcher, DispatchRepliesViaFacade)
{
    FakeNet net;
    Dispatcher d;
    int got_sum = 0;
    d.Register<RpcRequest>(MType::REQ_RPC, [&](INet &, uint64_t, RpcRequest::ptr &msg)
                           {
                               EXPECT_EQ(msg->Method(), "Add");
                               got_sum = msg->Params()["a"].asInt() + msg->Params()["b"].asInt();
                           });
    d.Register<RpcResponse>(MType::RSP_RPC, [&](INet &n, uint64_t id, RpcResponse::ptr &msg)
                            { n.Send(id, "ack-" + msg->Rid()); });

    RpcRequest::ptr req(new RpcRequest());
    req->SetMethod("Add");
    Json::Value p;
    p["a"] = 10;
    p["b"] = 32;
    req->SetParams(p);
    req->SetType(MType::REQ_RPC);
    Message::ptr msg = req;
    EXPECT_TRUE(d.Dispatch(net, 1, msg));
    EXPECT_EQ(got_sum, 42);

    RpcResponse::ptr rsp(new RpcResponse());
    rsp->SetRid("x9");
    rsp->SetType(MType::RSP_RPC);
    Message::ptr msg2 = rsp;
    EXPECT_TRUE(d.Dispatch(net, 7, msg2));
    auto sent = net.Sent();
    ASSERT_EQ(sent.size(), 1u); // 回话出口就是门面透传的 INet
    EXPECT_EQ(sent[0].first, 7u);
    EXPECT_EQ(sent[0].second, "ack-x9");

    // 无人认领的 mtype：Dispatch 只报 false，处置归调用方
    RpcRequest::ptr ghost(new RpcRequest());
    ghost->SetType(MType::REQ_RPC);
    d.Register<RpcResponse>(MType::RSP_RPC, [&](INet &, uint64_t, RpcResponse::ptr &) {});
    Dispatcher empty;
    Message::ptr msg3 = ghost;
    EXPECT_FALSE(empty.Dispatch(net, 1, msg3)); // 空表：没触发任何 handler，也没 Send
    EXPECT_EQ(net.Sent().size(), 1u);
}
