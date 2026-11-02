#include "rpc/rpc_router.hpp"
#include "protocol/protocol.hpp"

namespace nexus {
namespace rpc {

bool ServiceDescribe::Check(VType vtype, const Json::Value &val)
{
    switch (vtype)
    {
    case VType::BOOL:     return val.isBool();
    case VType::INTEGRAL: return val.isIntegral();
    case VType::NUMERIC:  return val.isNumeric();
    case VType::STRING:   return val.isString();
    case VType::ARRAY:    return val.isArray();
    case VType::OBJECT:   return val.isObject();
    }
    return false;
}

bool ServiceDescribe::ParamCheck(const Json::Value &params)
{
    if (!params.isObject())
        return false;
    for (auto &desc : _params_desc)
    {
        if (!params.isMember(desc.first))
            return false;
        if (!Check(desc.second, params[desc.first]))
            return false;
    }
    return true;
}

bool ServiceDescribe::Call(const Json::Value &params, Json::Value &result)
{
    _handler(params, result);
    return Check(_return_type, result);
}

void RpcRouter::RegisterMethod(const ServiceDescribe::ptr &service)
{
    std::unique_lock<std::shared_mutex> lock(_mutex);
    _services[service->Method()] = service;
}

void RpcRouter::OnRpcRequest(INet &net, uint64_t conn_id, RpcRequest::ptr &request)
{
    std::string method = request->Method();

    // 精确匹配
    ServiceDescribe::ptr svc;
    {
        std::shared_lock<std::shared_mutex> lock(_mutex);
        auto it = _services.find(method);
        if (it != _services.end())
            svc = it->second;
    }
    if (!svc)
    {
        return Response(net, conn_id, request->Rid(), Json::Value(), RCode::RCODE_NOT_FOUND_SERVICE);
    }
    if (!svc->ParamCheck(request->Params()))
    {
        return Response(net, conn_id, request->Rid(), Json::Value(), RCode::RCODE_INVALID_PARAMS);
    }

    Json::Value result;
    if (!svc->Call(request->Params(), result))
    {
        return Response(net, conn_id, request->Rid(), Json::Value(), RCode::RCODE_INTERNAL_ERROR);
    }
    Response(net, conn_id, request->Rid(), result, RCode::RCODE_OK);
}

void RpcRouter::Response(INet &net, uint64_t conn_id, const std::string &rid,
                         const Json::Value &result, RCode rcode)
{
    RpcResponse rsp;
    rsp.SetRid(rid);
    rsp.SetType(MType::RSP_RPC);
    rsp.SetRCode(rcode);
    rsp.SetResult(result);
    std::string wire = Encode((uint32_t)MType::RSP_RPC, rid, rsp.Serialize());
    net.Send(conn_id, wire);
}

} 
}
