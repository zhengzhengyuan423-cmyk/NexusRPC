// 用法: ./consumer_main [reg_ip] [reg_port] — 订 calc 轮询调用；kill -9 一个 provider 后自动切流
#include <algorithm>
#include <atomic>
#include <csignal>
#include <cstdlib>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include "logger.hpp"
#include "registry/registry_client.hpp"
#include "rpc/rpc_client.hpp"

using namespace std::chrono;
using namespace nexus;
using namespace nexus::rpc;

static std::atomic<bool> g_stop{false};
static void OnStop(int) { g_stop = true; }

int main(int argc, char *argv[])
{
    signal(SIGINT, OnStop);
    signal(SIGTERM, OnStop);
    std::string ip = argc > 1 ? argv[1] : "127.0.0.1";
    int reg_port = argc > 2 ? atoi(argv[2]) : 19890;

    std::mutex m;
    std::vector<std::string> pool;
    auto apply = [&](const std::string &inst, bool added)
    {
        std::lock_guard<std::mutex> lk(m);
        auto it = std::find(pool.begin(), pool.end(), inst);
        if (added && it == pool.end())
            pool.push_back(inst);
        if (!added && it != pool.end())
            pool.erase(it);
    };

    RegistryClient reg(ip, (uint16_t)reg_port);
    // 推送回调只改表；RpcClient 的销毁（含 join）留在主循环干，net 线程不许长阻塞
    reg.SetOnPush([&](const std::string &svc, const std::string &inst, bool added)
                  { apply(inst, added); });
    while (!reg.Connect() && !g_stop)
        std::this_thread::sleep_for(milliseconds(200));
    std::vector<std::string> snap;
    if (!reg.List("calc", snap) || !reg.Watch("calc"))
    {
        ELOG << "registry handshake failed";
        return 1;
    }
    for (auto &s : snap)
        apply(s, true);
    ILOG << "consumer watching calc, snapshot=" << snap.size();

    std::map<std::string, std::unique_ptr<RpcClient>> clients;
    size_t rr = 0;
    while (!g_stop)
    {
        {
            // 被摘除实例的长连接就地清算：连接池跟着订阅表走
            std::vector<std::string> dead;
            {
                std::lock_guard<std::mutex> lk(m);
                for (auto &[inst, cli] : clients)
                    if (std::find(pool.begin(), pool.end(), inst) == pool.end())
                        dead.push_back(inst);
            }
            for (auto &d : dead)
            {
                clients[d]->Stop();
                clients.erase(d);
            }
        }

        std::string target;
        {
            std::lock_guard<std::mutex> lk(m);
            if (!pool.empty())
                target = pool[rr++ % pool.size()];
        }
        if (target.empty())
        {
            ILOG << "no instance yet";
            for (int i = 0; i < 5 && !g_stop; i++)
                std::this_thread::sleep_for(milliseconds(100));
            continue;
        }

        auto &cli = clients[target];
        if (!cli)
        {
            size_t c = target.find(':');
            cli = std::make_unique<RpcClient>("127.0.0.1", (uint16_t)stoi(target.substr(c + 1)));
            if (!cli->Connect())
            {
                ILOG << "connect @" << target << " failed";
                clients.erase(target);
                continue;
            }
        }
        Json::Value p;
        p["a"] = 6;
        p["b"] = 7;
        RCode code;
        Json::Value result;
        if (cli->Call("Add", p, code, result))
            ILOG << "[consumer] Add(6,7)=" << result.asInt() << " @" << target;
        else
        {
            ILOG << "[consumer] @" << target << " failed, drop";
            cli->Stop();
            clients.erase(target);
        }
        for (int i = 0; i < 5 && !g_stop; i++)
            std::this_thread::sleep_for(milliseconds(100));
    }
    ILOG << "consumer exited cleanly";
    return 0;
}
