#include <gtest/gtest.h>
#include <chrono>
#include <cstring>
#include <thread>
#include "net/conn/socket.hpp"
#include "protocol/protocol.hpp"
#include "rpc/rpc_server.hpp"

using namespace nexus;
using namespace nexus::rpc;

class TestServer
{
public:
    TestServer(int port, int biz) : _srv(port, NetOptions{0, biz, 0})
    {
        _srv.RegisterService("Add", {{"a", VType::INTEGRAL}, {"b", VType::INTEGRAL}},
                             VType::INTEGRAL,
                             [](const Json::Value &p, Json::Value &r)
                             { r = p["a"].asInt() + p["b"].asInt(); });
        // 违约 handler：声明回 INTEGRAL 却回字符串，必须被 rtype check 拦下
        _srv.RegisterService("Bad", {}, VType::INTEGRAL,
                             [](const Json::Value &, Json::Value &r) { r = "oops"; });
        _th = std::thread([this]() { _srv.Start(); });
        WaitReady(port);
    }
    ~TestServer()
    {
        _srv.Stop();
        _th.join(); // 契约兑现点：Start 线程返回=收摊完成
    }

private:
    void WaitReady(int port)
    {
        for (int i = 0; i < 100; i++)
        {
            Socket probe;
            probe.Create();
            if (probe.Connect((uint16_t)port, "127.0.0.1"))
                return;
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        FAIL() << "server not ready";
    }

    RpcServer _srv;
    std::thread _th;
};

// 裸 socket 对手方：不经 RpcClient，直接拼帧、逐帧收账
struct RawPeer
{
    Socket cli;
    Buffer acc;

    void Connect(int port)
    {
        cli.Create();
        ASSERT_TRUE(cli.Connect((uint16_t)port, "127.0.0.1"));
    }

    void SendRequest(const std::string &method, const Json::Value &params, const std::string &rid)
    {
        RpcRequest req;
        req.SetRid(rid);
        req.SetMethod(method);
        req.SetParams(params);
        std::string wire = Encode((uint32_t)MType::REQ_RPC, rid, req.Serialize());
        cli.Send(wire.c_str(), wire.size());
    }

    // 错误回包的 result 是 null：先读 rcode，仅 OK 时才消费 result
    bool NextResponse(std::string &rid, RCode &code, Json::Value &result)
    {
        while (true)
        {
            FrameState st = PeekFrame(acc);
            if (st == FrameState::BAD)
                return false;
            if (st == FrameState::GOOD)
            {
                uint32_t mtype;
                std::string body;
                ParseFrame(acc, mtype, rid, body);
                if (mtype != (uint32_t)MType::RSP_RPC)
                    return false;
                RpcResponse rsp;
                if (!rsp.Unserialize(body))
                    return false;
                code = rsp.GetRCode();
                result = rsp.Result();
                return true;
            }
            char tmp[4096];
            ssize_t n = cli.Recv(tmp, sizeof(tmp));
            if (n <= 0)
                return false;
            acc.WriteStringAndPush(std::string(tmp, n));
        }
    }
};

TEST(Rpc, OkPathWithBizThreads)
{
    TestServer srv(19851, 2);
    RawPeer c;
    c.Connect(19851);
    Json::Value p;
    p["a"] = 3;
    p["b"] = 4;
    c.SendRequest("Add", p, "rid-1");

    std::string rid;
    RCode code;
    Json::Value result;
    ASSERT_TRUE(c.NextResponse(rid, code, result));
    EXPECT_EQ(rid, "rid-1"); // 回执带原 rid：请求-响应配对的依据
    EXPECT_EQ(code, RCode::RCODE_OK);
    EXPECT_EQ(result.asInt(), 7);
}

TEST(Rpc, ExactMatchNoPrefix)
{
    TestServer srv(19852, 0);
    RawPeer c;
    c.Connect(19852);
    Json::Value p;
    p["a"] = 1;
    p["b"] = 2;
    c.SendRequest("AddEx", p, "rid-a"); // 前缀多出一截
    c.SendRequest("Ad", p, "rid-b");    // 前缀少一截

    std::string rid;
    RCode code;
    Json::Value result;
    ASSERT_TRUE(c.NextResponse(rid, code, result));
    EXPECT_EQ(code, RCode::RCODE_NOT_FOUND_SERVICE);
    ASSERT_TRUE(c.NextResponse(rid, code, result));
    EXPECT_EQ(code, RCode::RCODE_NOT_FOUND_SERVICE);
}

// 参数类型违约与 handler 返回值违约：都挡在响应路径上，脏数据不出门
TEST(Rpc, RejectBadParams)
{
    TestServer srv(19853, 0);
    RawPeer c;
    c.Connect(19853);
    Json::Value p;
    p["a"] = "x"; // 声明 INTEGRAL 实给 STRING
    p["b"] = "y";
    c.SendRequest("Add", p, "rid-1");

    std::string rid;
    RCode code;
    Json::Value result;
    ASSERT_TRUE(c.NextResponse(rid, code, result));
    EXPECT_EQ(code, RCode::RCODE_INVALID_PARAMS); // handler 根本没被调用

    c.SendRequest("Bad", Json::Value(Json::objectValue), "rid-2");
    ASSERT_TRUE(c.NextResponse(rid, code, result));
    EXPECT_EQ(code, RCode::RCODE_INTERNAL_ERROR); // 脏返回值不流向调用方
}

TEST(Rpc, GarbageBytesKillConn)
{
    TestServer srv(19856, 0);
    Socket cli;
    cli.Create();
    ASSERT_TRUE(cli.Connect(19856, "127.0.0.1"));
    const char *junk = "hello world not a frame at all";
    cli.Send(junk, strlen(junk)); // 前 4 字节当长度头是天文数字：BAD 帧
    char tmp[64];
    EXPECT_EQ(cli.Recv(tmp, sizeof(tmp)), 0); // 对端已关：EOF 为证
}
