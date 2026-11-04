#pragma once
#include <memory>
#include "protocol/dispatcher.hpp"
#include "protocol/pump.hpp"
#include "rpc/rpc_router.hpp"
#include "sentinel/sentinel.hpp"

namespace nexus {
namespace rpc {

class RpcServer
{
public:
    explicit RpcServer(int port, NetOptions opts = {});

    void RegisterService(const std::string &method,
                         const std::vector<ServiceDescribe::ParamsDescribe> &desc,
                         VType return_type, const ServiceDescribe::ServiceCallback &handler);
    void Start(); // 阻塞，直到 Stop 触发收摊
    void Stop() { _net.Stop(); }

    // 治理按方法登记；限流排队在服务端强制失效
    void ConfigureGovernance(const std::string &method, const Sentinel::LimiterCfg &lmt,
                             const Sentinel::BreakerCfg &bkr = {});

private:
    bool OnFrame(uint64_t conn_id, uint32_t mtype,
                 const std::string &rid, const std::string &body);

    NetServer _net;
    Sentinel _gov; // 生命周期随 server，指针在构造里交给 router
    RpcRouter::ptr _router;
    Dispatcher _dispatcher;
    std::unique_ptr<FramePump> _pump;
};

} 

}
