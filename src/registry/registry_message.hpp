#pragma once
#include <string>
#include "protocol/message.hpp"

namespace nexus {

// 注册中心三帧，全部 LV+JSON 通道，字段名共用
class RegistryRequest : public JsonMessage
{
public:
    using ptr = std::shared_ptr<RegistryRequest>;
    enum Kind
    {
        REGISTER = 1,
        HEARTBEAT,
        LIST,
        WATCH
    };
    bool Check() override;
    void SetOp(Kind op) { _body[key::OP] = (int)op; }
    int Op() { return _body[key::OP].asInt(); }
    void SetService(const std::string &s) { _body[key::SERVICE] = s; }
    std::string Service() { return _body[key::SERVICE].asString(); }
    void SetInstance(const std::string &s) { _body[key::INSTANCE] = s; }
    std::string Instance() { return _body[key::INSTANCE].asString(); }
};

class RegistryResponse : public JsonMessage
{
public:
    using ptr = std::shared_ptr<RegistryResponse>;
    bool Check() override;
    void SetCode(RCode c) { _body[key::RCODE] = (int)c; }
    RCode Code() { return (RCode)_body[key::RCODE].asInt(); }
    void SetData(const Json::Value &d) { _body[key::DATA] = d; }
    Json::Value Data() { return _body[key::DATA]; }
};

class RegistryPush : public JsonMessage
{
public:
    using ptr = std::shared_ptr<RegistryPush>;
    bool Check() override;
    void Set(const std::string &service, const std::string &instance, bool added)
    {
        _body[key::SERVICE] = service;
        _body[key::INSTANCE] = instance;
        _body[key::ADDED] = added;
    }
    std::string Service() { return _body[key::SERVICE].asString(); }
    std::string Instance() { return _body[key::INSTANCE].asString(); }
    bool Added() { return _body[key::ADDED].asBool(); }
};

// 三帧挂进 MessageFactory；幂等，构造 server/client 前必调
void EnsureRegistryMessages();

}
