#pragma once
#include <functional>
#include <jsoncpp/json/json.h>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "net/api/inet.hpp"
#include "protocol/dispatcher.hpp"
#include "protocol/message.hpp"

namespace nexus {
namespace rpc {

// 参数/返回值类型描述，对应 jsoncpp 的 Value 类型族
enum class VType
{
    BOOL = 0,
    INTEGRAL,
    NUMERIC,
    STRING,
    ARRAY,
    OBJECT,
};

// 一个 RPC 方法的完整契约：方法名 + 入参类型表 + 返回类型 + handler
class ServiceDescribe
{
public:
    using ptr = std::shared_ptr<ServiceDescribe>;
    using ServiceCallback = std::function<void(const Json::Value &, Json::Value &)>;
    using ParamsDescribe = std::pair<std::string, VType>;

    ServiceDescribe(const std::string &method, const std::vector<ParamsDescribe> &desc,
                    VType return_type, const ServiceCallback &handler)
        : _method_name(method), _params_desc(desc), _return_type(return_type), _handler(handler) {}

    const std::string &Method() { return _method_name; }
    bool ParamCheck(const Json::Value &params);
    // handler 执行 + 返回值类型校验：类型不对调用方拿到 INTERNAL_ERROR 而非脏数据
    bool Call(const Json::Value &params, Json::Value &result);

private:
    static bool Check(VType vtype, const Json::Value &val);

    std::string _method_name;
    std::vector<ParamsDescribe> _params_desc;
    VType _return_type;
    ServiceCallback _handler;
};

class RpcRouter
{
public:
    using ptr = std::shared_ptr<RpcRouter>;
    void RegisterMethod(const ServiceDescribe::ptr &service);

    void OnRpcRequest(INet &net, uint64_t conn_id, RpcRequest::ptr &request);

private:
    static void Response(INet &net, uint64_t conn_id, const std::string &rid,
                         const Json::Value &result, RCode rcode);

    std::unordered_map<std::string, ServiceDescribe::ptr> _services;
    mutable std::shared_mutex _mutex;
};

} 
}
