#include "rpc/rpc_router.hpp"
#include <chrono>
#include "protocol/protocol.hpp"
#include "sentinel/sentinel.hpp"

namespace nexus {
namespace rpc {

namespace
{
int64_t NowMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}
}

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

    // 闸门排在查表之后：查无此方法不是业务失败，不该扣任何人的额度
    if (_gov)
    {
        int64_t wait = 0;
        if (!_gov->Entry(method, NowMs(), wait))
            return Response(net, conn_id, request->Rid(), Json::Value(), RCode::RCODE_LIMITED);
    }

    int64_t t0 = NowMs();
    if (!svc->ParamCheck(request->Params()))
    {
        Response(net, conn_id, request->Rid(), Json::Value(), RCode::RCODE_INVALID_PARAMS);
        // 参数违约是调用方的事：记成功账，不该拉低被调方熔断胜率
        if (_gov)
            _gov->Exit(method, true, 0, NowMs());
        return;
    }

    Json::Value result;
    bool ok = svc->Call(request->Params(), result);
    // Entry 过了闸的请求必须 Exit
    if (_gov)
        _gov->Exit(method, ok, NowMs() - t0, NowMs());

    if (!ok)
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
