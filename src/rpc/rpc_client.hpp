#pragma once
#include <future>
#include <memory>
#include <string>
#include "protocol/pump.hpp"
#include "protocol/requestor.hpp"
#include "rpc/rpc_router.hpp"

namespace nexus {
namespace rpc {

// NetClient(连接) + FramePump(攒帧) + Requestor(配对)
class RpcClient
{
public:
    RpcClient(const std::string &ip, uint16_t port);
    ~RpcClient();

    bool Connect();
    void Stop(); 

    // 同步：就地阻塞，回包或断线才醒。false=断线/协议错/链路不在
    bool Call(const std::string &method, const Json::Value &params,
              RCode &code, Json::Value &result);
    // future：拿回等待凭证，兑现时机调用方自定
    bool Call(const std::string &method, const Json::Value &params,
              std::future<Message::ptr> &fut);
    // 回调：回包线程执行 cb；nullptr=断线
    bool Call(const std::string &method, const Json::Value &params,
              const Requestor::Callback &cb);

private:
    std::string BuildRequest(const std::string &method, const Json::Value &params,
                             const std::string &rid);
    bool OnFrame(uint64_t conn_id, uint32_t mtype,
                 const std::string &rid, const std::string &body);

    NetClient _net; 
    std::unique_ptr<FramePump> _pump;
    std::shared_ptr<Requestor> _requestor;
};

} 
}
