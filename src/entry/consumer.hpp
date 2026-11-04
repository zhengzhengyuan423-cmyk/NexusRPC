#pragma once
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include "registry/registry_client.hpp"
#include "rpc/rpc_client.hpp"
#include "sentinel/sentinel.hpp"

namespace nexus {

// 消费者入口：订名单 + 连接池 + 轮询选择三合一。

class Consumer
{
public:
    Consumer(const std::string &reg_ip, uint16_t reg_port);
    ~Consumer();

    // Connect + List 快照 + Watch 订推送。此后名单跟着 registry 的 push 实时变
    bool Subscribe(const std::string &service);
    // 过闸→轮询选实例直连调用。false=名单空、连不上、或该实例断线（已就地摘牌）；
    // 本地闸门拒也回 true：LIMITED 是回执，不是传输失败
    bool Call(const std::string &method, const Json::Value &params,
              RCode &code, Json::Value &result);
    // 与 Provider 版签名一致，唯一区别：这里的 queueing_ms 有效——排队睡的是业务自己的线程
    void ConfigureGovernance(const std::string &method, const Sentinel::LimiterCfg &lmt,
                             const Sentinel::BreakerCfg &bkr = {});
    void Stop(); // 幂等

private:
    void OnPush(const std::string &service, const std::string &instance, bool added);

    struct Entry
    {
        std::shared_ptr<rpc::RpcClient> cli;
        bool ready = false; // false=有线程正在 Connect，绕到者换下一个实例
    };

    std::string _service; // Subscribe 开头写死，先于一切 push 回调，无竞争
    Sentinel _gov;
    RegistryClient _reg;
    std::mutex _mutex; // 护 _names/_pool：push 在 reg net 线程写，Call 在业务线程读写
    std::vector<std::string> _names;
    std::unordered_map<std::string, Entry> _pool;
    size_t _rr = 0;
};

}
