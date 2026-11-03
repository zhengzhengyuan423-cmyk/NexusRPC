// 用法: ./provider_main [reg_ip] [reg_port] [rpc_port] — 自登记自心跳，对外提供 Add/Sub
#include <atomic>
#include <csignal>
#include <cstdlib>
#include <string>
#include <thread>
#include "logger.hpp"
#include "registry/registry_client.hpp"
#include "rpc/rpc_server.hpp"

using namespace std::chrono;
using namespace nexus;
using namespace nexus::rpc;

static std::atomic<bool> g_stop{false};
static RpcServer *g_rpc = nullptr;
static void OnStop(int)
{
    g_stop = true;
    if (g_rpc)
        g_rpc->Stop(); // C2 任意线程可调
}

int main(int argc, char *argv[])
{
    std::string ip = argc > 1 ? argv[1] : "127.0.0.1";
    int reg_port = argc > 2 ? atoi(argv[2]) : 19890;
    int rpc_port = argc > 3 ? atoi(argv[3]) : 19891;

    RpcServer srv(rpc_port, NetOptions{1, 2, 0});
    g_rpc = &srv;
    signal(SIGINT, OnStop);
    signal(SIGTERM, OnStop);
    srv.RegisterService("Add", {{"a", VType::INTEGRAL}, {"b", VType::INTEGRAL}},
                        VType::INTEGRAL,
                        [](const Json::Value &p, Json::Value &r)
                        { r = p["a"].asInt() + p["b"].asInt(); });
    srv.RegisterService("Sub", {{"a", VType::INTEGRAL}, {"b", VType::INTEGRAL}},
                        VType::INTEGRAL,
                        [](const Json::Value &p, Json::Value &r)
                        { r = p["a"].asInt() - p["b"].asInt(); });
    std::thread rpc_th([&] { srv.Start(); });

    std::string inst = "127.0.0.1:" + std::to_string(rpc_port);
    RegistryClient reg(ip, (uint16_t)reg_port);
    while (!reg.Connect() && !g_stop)
        std::this_thread::sleep_for(milliseconds(200));

    // 心跳即自检：回包 NOT_FOUND（registry 重启过）或断线就地重登记
    while (!g_stop)
    {
        if (!reg.Heartbeat("calc", inst) && reg.Register("calc", inst))
            ILOG << "provider " << inst << " online";
        for (int i = 0; i < 10 && !g_stop; i++)
            std::this_thread::sleep_for(milliseconds(100));
    }
    srv.Stop();
    rpc_th.join();
    ILOG << "provider " << inst << " exited cleanly";
    return 0;
}
