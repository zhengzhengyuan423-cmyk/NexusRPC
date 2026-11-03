#include "rpc/rpc_client.hpp"
#include "protocol/protocol.hpp"

namespace nexus {
namespace rpc {

RpcClient::RpcClient(const std::string &ip, uint16_t port) : _net(ip, port)
{
    _requestor = std::make_shared<Requestor>(_net);
    _pump.reset(new FramePump(
        _net,
        [this](uint64_t id, uint32_t mtype, const std::string &rid, const std::string &body)
        { return OnFrame(id, mtype, rid, body); }));
    _net.SetOnMessage([this](uint64_t id, const std::string &bytes)
                      { _pump->Feed(id, bytes); });
    _net.SetOnClose([this](uint64_t)
                    { _requestor->FailAll(); });
}

RpcClient::~RpcClient()
{
    Stop(); // 收摊必须在成员死之前：loop 排空期间回调还够得着 _pump/_requestor
}

bool RpcClient::Connect() { return _net.Connect(); }

void RpcClient::Stop()
{
    _requestor->FailAll();
    _net.Stop(); // 同步：返回后不再有任何回调
}

std::string RpcClient::BuildRequest(const std::string &method, const Json::Value &params,
                                    const std::string &rid)
{
    RpcRequest req;
    req.SetRid(rid);
    req.SetMethod(method);
    req.SetParams(params);
    return Encode((uint32_t)MType::REQ_RPC, rid, req.Serialize());
}

bool RpcClient::OnFrame(uint64_t, uint32_t mtype,
                        const std::string &rid, const std::string &body)
{
    if (mtype != (uint32_t)MType::RSP_RPC)
        return false; // 客户端不该收到请求帧
    RpcResponse::ptr rsp(new RpcResponse());
    rsp->SetRid(rid);
    rsp->SetType(MType::RSP_RPC);
    if (!rsp->Unserialize(body))
        return false;
    // 不跑 Check：错误回包的 result 就是 null，那是业务层信息；
    // 传输层只保证"这是个语法正确的响应"，rcode 由调用方判读
    Message::ptr msg = rsp;
    _requestor->OnResponse(rid, msg);
    return true;
}

bool RpcClient::Call(const std::string &method, const Json::Value &params,
                     RCode &code, Json::Value &result)
{
    std::string rid = _requestor->NextRid();
    Message::ptr rsp;
    if (!_requestor->Send(_net.Id(), BuildRequest(method, params, rid), rid, rsp))
        return false;
    RpcResponse::ptr typed = std::dynamic_pointer_cast<RpcResponse>(rsp);
    if (!typed)
        return false;
    code = typed->GetRCode();
    result = typed->Result();
    return true;
}

bool RpcClient::Call(const std::string &method, const Json::Value &params,
                     std::future<Message::ptr> &fut)
{
    std::string rid = _requestor->NextRid();
    return _requestor->Send(_net.Id(), BuildRequest(method, params, rid), rid, fut);
}

bool RpcClient::Call(const std::string &method, const Json::Value &params,
                     const Requestor::Callback &cb)
{
    std::string rid = _requestor->NextRid();
    return _requestor->Send(_net.Id(), BuildRequest(method, params, rid), rid, cb);
}

} 
}
