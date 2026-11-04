#include "rpc/rpc_server.hpp"
#include "logger.hpp"

namespace nexus {
namespace rpc {

RpcServer::RpcServer(int port, NetOptions opts)
    : _net((uint16_t)port, opts), _router(std::make_shared<RpcRouter>())
{
    _router->SetGovernance(&_gov);
    _pump.reset(new FramePump(
        _net,
        [this](uint64_t id, uint32_t mtype, const std::string &rid, const std::string &body)
        { return OnFrame(id, mtype, rid, body); }));
}

void RpcServer::RegisterService(const std::string &method,
                                const std::vector<ServiceDescribe::ParamsDescribe> &desc,
                                VType return_type, const ServiceDescribe::ServiceCallback &handler)
{
    _router->RegisterMethod(std::make_shared<ServiceDescribe>(method, desc, return_type, handler));
}

void RpcServer::ConfigureGovernance(const std::string &method, const Sentinel::LimiterCfg &lmt,
                                    const Sentinel::BreakerCfg &bkr)
{
    Sentinel::LimiterCfg c = lmt;
    c.queueing_ms = 0; // 服务端只判不排队：想排队是调用方的事
    _gov.Configure(method, c, bkr);
}

void RpcServer::Start()
{
    _dispatcher.Register<RpcRequest>(
        MType::REQ_RPC,
        [this](INet &net, uint64_t id, RpcRequest::ptr &req)
        { _router->OnRpcRequest(net, id, req); });
    _net.SetOnMessage([this](uint64_t id, const std::string &bytes)
                      { _pump->Feed(id, bytes); });
    // C1 契约兜住保序：on_close 必在该连接全部 on_message 之后
    _net.SetOnClose([this](uint64_t id)
                    { _pump->Forget(id); });
    _net.Start();
}

bool RpcServer::OnFrame(uint64_t conn_id, uint32_t mtype,
                        const std::string &rid, const std::string &body)
{
    Message::ptr msg = MessageFactory::Create(mtype);
    if (!msg)
        return false; // 未登记的 mtype：按坏帧处决
    msg->SetRid(rid);
    msg->SetType((MType)mtype);
    if (!msg->Unserialize(body) || !msg->Check())
        return false;
    return _dispatcher.Dispatch(_net, conn_id, msg); // false=帧合法但没人认领
}

} 
}
