// 用法: ./rpc_bench [direct|consumer] [threads=10] [per_thread=1000] [biz=2] [qps=0]

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <vector>
#include "logger.hpp"
#include "net/conn/socket.hpp"
#include "entry/consumer.hpp"
#include "entry/provider.hpp"
#include "registry/registry_server.hpp"
#include "rpc/rpc_client.hpp"
#include "rpc/rpc_server.hpp"

using namespace std::chrono;
using namespace nexus;
using namespace nexus::rpc;

namespace
{
// RpcServer 和 Provider 的 RegisterService 签名一致
template <class S>
void AddService(S &s)
{
    s.RegisterService("Add", {{"a", VType::INTEGRAL}, {"b", VType::INTEGRAL}},
                      VType::INTEGRAL,
                      [](const Json::Value &prm, Json::Value &r)
                      { r = prm["a"].asInt() + prm["b"].asInt(); });
}

void WaitPort(int port)
{
    for (int i = 0; i < 100; i++)
    {
        Socket probe;
        probe.Create();
        if (probe.Connect(port, "127.0.0.1"))
            return;
        std::this_thread::sleep_for(milliseconds(50));
    }
    printf("port %d not ready\n", port);
    exit(1);
}

// 每线程一条直连 RpcClient 或一个 Consumer，闭环串行打
struct Bench
{
    bool consumer_mode;
    int per;
    uint16_t port;
    uint16_t reg_port;
    double qps;
    std::atomic<long long> ok{0}, limited{0}, fail{0};
    std::vector<std::vector<int64_t>> lat;

    // caller 只要有 Call(method, params, code, result)：RpcClient 和 Consumer 签名一致
    template <class C>
    void Worker(C &caller, int t)
    {
        Json::Value prm, r;
        prm["a"] = 1;
        prm["b"] = 2;
        RCode code;
        lat[t].reserve(per);
        for (int i = 0; i < per; i++)
        {
            auto s = steady_clock::now();
            bool transport = caller.Call("Add", prm, code, r);
            int64_t d = duration_cast<microseconds>(steady_clock::now() - s).count();
            if (!transport || code == RCode::RCODE_INTERNAL_ERROR)
                fail++;
            else if (code == RCode::RCODE_LIMITED)
            {
                limited++;
                if (consumer_mode)
                    std::this_thread::sleep_for(milliseconds(1)); // 本地拒不碰网络，退避才能跨过额度窗口
                else
                    lat[t].push_back(d); // direct 的 LIMITED 走了网络往返，计延迟
            }
            else
            {
                ok++;
                lat[t].push_back(d);
            }
        }
    }

    void DirectThread(int t)
    {
        RpcClient cli("127.0.0.1", port);
        if (!cli.Connect())
        {
            fail += per;
            return;
        }
        Worker(cli, t);
    }

    void ConsumerThread(int t)
    {
        Consumer c("127.0.0.1", reg_port);
        if (!c.Subscribe("calc"))
        {
            fail += per;
            return;
        }
        if (qps > 0)
        {
            Sentinel::LimiterCfg lmt;
            lmt.enabled = true;
            lmt.qps = qps;
            c.ConfigureGovernance("Add", lmt);
        }
        Json::Value prm, r;
        prm["a"] = 1;
        prm["b"] = 2;
        RCode code;
        c.Call("Add", prm, code, r); // 预热：建连与名单到位
        Worker(c, t);
    }

    double RunWorkers(int threads)
    {
        auto t0 = steady_clock::now();
        std::vector<std::thread> ws;
        for (int t = 0; t < threads; t++)
            ws.emplace_back(consumer_mode ? &Bench::ConsumerThread : &Bench::DirectThread,
                            this, t);
        for (auto &w : ws)
            w.join();
        return duration_cast<microseconds>(steady_clock::now() - t0).count() / 1e6;
    }
};
}

int main(int argc, char *argv[])
{
    _logger.EnableFileLogStrategy(); // 日志进 ./log/my.log，stdout 只留测量值
    bool consumer = argc > 1 && strcmp(argv[1], "consumer") == 0;
    int threads = argc > 2 ? atoi(argv[2]) : 10;
    int per = argc > 3 ? atoi(argv[3]) : 1000;
    int biz = argc > 4 ? atoi(argv[4]) : 2;
    double qps = argc > 5 ? atof(argv[5]) : 0.0;
    const uint16_t port = 19910, reg_port = 19911, prov_port = 19912;

    Bench b{consumer, per, port, reg_port, qps};
    b.lat.resize(threads);

    double wall = 0;
    if (consumer)
    {
        RegistryServer reg(reg_port);
        std::thread rth([&] { reg.Start(); });
        WaitPort(reg_port);
        Provider p1("calc", prov_port, "127.0.0.1", reg_port);
        Provider p2("calc", prov_port + 1, "127.0.0.1", reg_port);
        AddService(p1);
        AddService(p2);
        p1.Start();
        p2.Start();
        wall = b.RunWorkers(threads);
        p1.Stop();
        p2.Stop();
        reg.Stop();
        rth.join();
    }
    else
    {
        RpcServer srv(port, NetOptions{0, biz, 0});
        AddService(srv);
        if (qps > 0)
        {
            Sentinel::LimiterCfg lmt;
            lmt.enabled = true;
            lmt.qps = qps;
            srv.ConfigureGovernance("Add", lmt);
        }
        std::thread th([&] { srv.Start(); });
        WaitPort(port);
        wall = b.RunWorkers(threads);
        srv.Stop();
        th.join();
    }

    std::vector<int64_t> all;
    size_t total = 0;
    for (auto &v : b.lat)
        total += v.size();
    all.reserve(total);
    for (auto &v : b.lat)
        all.insert(all.end(), v.begin(), v.end());
    long long attempts = (long long)b.ok + b.limited + b.fail;
    double sum = 0;
    long long mx = 0;
    for (auto v : all)
    {
        sum += (double)v;
        if (v > mx)
            mx = v;
    }

    printf("mode=%s threads=%d per=%d biz=%d qps=%.0f\n", consumer ? "consumer" : "direct",
           threads, per, consumer ? 2 : biz, qps);
    printf("attempts=%lld samples=%zu ok=%lld limited=%lld fail=%lld wall=%.3fs\n", attempts,
           all.size(), (long long)b.ok, (long long)b.limited, (long long)b.fail, wall);
    printf("throughput=%.0f/s ok_rate=%.0f/s limited_rate=%.0f/s\n",
           attempts / wall, b.ok.load() / wall, b.limited.load() / wall);
    if (!all.empty())
        printf("lat_us avg=%.0f max=%lld\n", sum / all.size(), mx);
    return 0;
}
