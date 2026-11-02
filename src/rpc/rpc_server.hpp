#pragma once
#include <memory>
#include "protocol/dispatcher.hpp"
#include "protocol/pump.hpp"
#include "rpc/rpc_router.hpp"

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

private:
    bool OnFrame(uint64_t conn_id, uint32_t mtype,
                 const std::string &rid, const std::string &body);

    NetServer _net;
    RpcRouter::ptr _router;
    Dispatcher _dispatcher;
    std::unique_ptr<FramePump> _pump;
};

} 

}
