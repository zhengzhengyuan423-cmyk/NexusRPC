#pragma once
#include <functional>
#include <memory>
#include <string>
#include <vector>
#include "protocol/pump.hpp"
#include "protocol/requestor.hpp"
#include "registry/registry_message.hpp"

namespace nexus {

// 注册中心客户端 SDK：provider 用它登记心跳，consumer 用它查表订推送
class RegistryClient
{
public:
    using PushFn = std::function<void(const std::string &service,
                                      const std::string &instance, bool added)>;

    RegistryClient(const std::string &ip, uint16_t port);
    ~RegistryClient();

    bool Connect();
    void Stop(); // 显式 FailAll 再收摊；之后可再 Connect = 重连
    void SetOnPush(const PushFn &fn) { _on_push = fn; } // Connect 前注册；net 线程只读

    bool Register(const std::string &service, const std::string &instance);
    bool Heartbeat(const std::string &service, const std::string &instance); // false=链路断，或登记不在表（registry 重启过）
    bool List(const std::string &service, std::vector<std::string> &out);
    bool Watch(const std::string &service);

private:
    bool Call(int op, const std::string &service, const std::string &instance,
              RCode &code, Json::Value &data);
    bool OnFrame(uint64_t conn_id, uint32_t mtype, const std::string &rid, const std::string &body);

    NetClient _net; 
    std::unique_ptr<FramePump> _pump;
    std::shared_ptr<Requestor> _requestor;
    PushFn _on_push;
};

}
