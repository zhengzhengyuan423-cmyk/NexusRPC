#pragma once
#include <functional>
#include <jsoncpp/json/json.h>
#include <memory>
#include <string>
#include "protocol/fields.hpp"

namespace nexus {

class Message
{
public:
    using ptr = std::shared_ptr<Message>;
    virtual ~Message() = default;
    virtual std::string Serialize() = 0;
    virtual bool Unserialize(const std::string &body) = 0;
    virtual bool Check() = 0;

    void SetRid(const std::string &rid) { _rid = rid; }
    std::string Rid() { return _rid; }
    void SetType(MType t) { _mtype = t; }
    MType GetType() { return _mtype; }

private:
    MType _mtype = MType::REQ_RPC;
    std::string _rid;
};

class JsonMessage : public Message
{
public:
    using ptr = std::shared_ptr<JsonMessage>;
    std::string Serialize() override;
    bool Unserialize(const std::string &body) override;

protected:
    Json::Value _body;
};

class RpcRequest : public JsonMessage
{
public:
    using ptr = std::shared_ptr<RpcRequest>;
    bool Check() override;
    std::string Method() { return _body[key::METHOD].asString(); }
    void SetMethod(const std::string &m) { _body[key::METHOD] = m; }
    Json::Value Params() { return _body[key::PARAMS]; }
    void SetParams(const Json::Value &p) { _body[key::PARAMS] = p; }
};

class RpcResponse : public JsonMessage
{
public:
    using ptr = std::shared_ptr<RpcResponse>;
    bool Check() override;
    RCode GetRCode() { return (RCode)_body[key::RCODE].asInt(); }
    void SetRCode(RCode c) { _body[key::RCODE] = (int)c; }
    Json::Value Result() { return _body[key::RESULT]; }
    void SetResult(const Json::Value &r) { _body[key::RESULT] = r; }
};

class MessageFactory
{
public:
    using Creator = std::function<Message::ptr()>;
    // 上层协议自登记，protocol 不必认识它们；
    // 只在 Start 之前调用，运行期只读不加锁
    static void Register(MType mtype, const Creator &creator);
    // 收到未登记的 mtype 返回 nullptr：调用方按坏帧处理
    static Message::ptr Create(uint32_t mtype);
};

}
