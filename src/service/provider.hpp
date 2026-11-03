#pragma once
#include <atomic>
#include <string>
#include <thread>
#include <vector>
#include "registry/registry_client.hpp"
#include "rpc/rpc_server.hpp"

namespace nexus {

// 注册者入口：RpcServer + 自登记 + 心跳线程三合一。

class Provider
{
public:
    Provider(const std::string &service, uint16_t rpc_port,
             const std::string &reg_ip, uint16_t reg_port);
    ~Provider();

    // 透传 RpcServer，签名一致
    void RegisterService(const std::string &method,
                         const std::vector<rpc::ServiceDescribe::ParamsDescribe> &desc,
                         rpc::VType return_type,
                         const rpc::ServiceDescribe::ServiceCallback &handler);
    void Start(); // 起服务线程，连注册中心、登记，再开心跳线程
    void Stop();  // 幂等：只有第一次真正收摊

private:
    void HeartbeatLoop();

    std::string _service;
    std::string _inst;
    rpc::RpcServer _server;
    RegistryClient _reg;
    std::thread _loop_th;
    std::thread _hb_th;
    std::atomic<bool> _running{false};
};

}
