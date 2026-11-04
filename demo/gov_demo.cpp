// 用法: ./gov_demo — 单进程跑通治理三场景：服务端限流 / 熔断自愈 / 调用方排队
#include <atomic>
#include <chrono>
#include <thread>
#include "logger.hpp"
#include "net/conn/socket.hpp"
#include "entry/consumer.hpp"
#include "entry/provider.hpp"
#include "registry/registry_server.hpp"

using namespace std::chrono;
using namespace nexus;
using namespace nexus::rpc;

int main()
{
    RegistryServer reg(19900);
    std::thread reg_th([&] { reg.Start(); });
    for (int i = 0; i < 100; i++)
    {
        Socket probe;
        probe.Create();
        if (probe.Connect(19900, "127.0.0.1"))
            break;
        std::this_thread::sleep_for(milliseconds(50));
    }

    Provider p("calc", 19901, "127.0.0.1", 19900);
    p.RegisterService("Add", {{"a", VType::INTEGRAL}, {"b", VType::INTEGRAL}}, VType::INTEGRAL,
                      [](const Json::Value &prm, Json::Value &r)
                      { r = prm["a"].asInt() + prm["b"].asInt(); });
    std::atomic<bool> poison{true}; // 让 Flaky 违约的开关，稍后演示"服务自愈"
    p.RegisterService("Flaky", {}, VType::INTEGRAL,
                      [&](const Json::Value &, Json::Value &r)
                      { r = poison.load() ? Json::Value("oops") : Json::Value(1); });

    Sentinel::LimiterCfg lmt;
    lmt.enabled = true;
    lmt.qps = 2;
    p.ConfigureGovernance("Add", lmt);
    Sentinel::BreakerCfg bkr;
    bkr.enabled = true;
    bkr.min_request = 2;
    bkr.recovery_ms = 1000;
    p.ConfigureGovernance("Flaky", Sentinel::LimiterCfg(), bkr);
    p.Start();

    Consumer c("127.0.0.1", 19900);
    if (!c.Subscribe("calc"))
    {
        ELOG << "subscribe failed";
        return 1;
    }

    Json::Value prm;
    prm["a"] = 6;
    prm["b"] = 7;
    RCode code;
    Json::Value r;

    ILOG << "== 场景A：服务端限流 qps=2，连打 6 发 ==";
    int ok = 0;
    int limited = 0;
    for (int i = 0; i < 6; i++)
    {
        c.Call("Add", prm, code, r);
        code == RCode::RCODE_OK ? ok++ : limited++;
    }
    ILOG << "  ok=" << ok << " limited=" << limited;

    ILOG << "== 场景B：熔断。前两笔违约 ==";
    c.Call("Flaky", Json::Value(Json::objectValue), code, r);
    ILOG << "  #1 " << ErrReason(code);
    c.Call("Flaky", Json::Value(Json::objectValue), code, r);
    ILOG << "  #2 " << ErrReason(code) << "（两笔后开窗）";
    c.Call("Flaky", Json::Value(Json::objectValue), code, r);
    ILOG << "  #3 " << ErrReason(code) << "（开窗期间 handler 不挨打）";
    poison = false;
    std::this_thread::sleep_for(milliseconds(1100));
    c.Call("Flaky", Json::Value(Json::objectValue), code, r);
    ILOG << "  #4 " << ErrReason(code) << "（半开探测通过，归闭）";

    ILOG << "== 场景C：调用方排队 qps=1，连拨 3 发 ==";
    Sentinel::LimiterCfg pace;
    pace.enabled = true;
    pace.qps = 1;
    pace.queueing_ms = 3000;
    c.ConfigureGovernance("Add", pace);
    for (int i = 0; i < 3; i++)
    {
        auto t0 = steady_clock::now();
        c.Call("Add", prm, code, r);
        ILOG << "  #" << i + 1 << " " << ErrReason(code)
             << " 耗时=" << duration_cast<milliseconds>(steady_clock::now() - t0).count() << "ms";
    }

    c.Stop();
    p.Stop();
    reg.Stop();
    reg_th.join();
    return 0;
}
