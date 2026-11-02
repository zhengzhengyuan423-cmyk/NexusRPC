#include "protocol/message.hpp"
#include <sstream>
#include <unordered_map>

namespace nexus {

static bool DumpJson(const Json::Value &val, std::string &out)
{
    std::stringstream ss;
    Json::StreamWriterBuilder swb;
    std::unique_ptr<Json::StreamWriter> sw(swb.newStreamWriter());
    if (sw->write(val, &ss) != 0)
        return false;
    out = ss.str();
    return true;
}

static bool LoadJson(const std::string &body, Json::Value &val)
{
    Json::CharReaderBuilder crb;
    std::string errs;
    std::unique_ptr<Json::CharReader> cr(crb.newCharReader());
    return cr->parse(body.c_str(), body.c_str() + body.size(), &val, &errs);
}

std::string JsonMessage::Serialize()
{
    std::string body;
    if (!DumpJson(_body, body))
        return std::string();
    return body;
}

bool JsonMessage::Unserialize(const std::string &body)
{
    return LoadJson(body, _body);
}

bool RpcRequest::Check()
{
    if (_body[key::METHOD].isNull() || !_body[key::METHOD].isString())
        return false;
    if (_body[key::PARAMS].isNull() || !_body[key::PARAMS].isObject())
        return false;
    return true;
}

bool RpcResponse::Check()
{
    if (_body[key::RCODE].isNull() || !_body[key::RCODE].isIntegral())
        return false;
    if (_body[key::RESULT].isNull())
        return false;
    return true;
}

static std::unordered_map<uint32_t, MessageFactory::Creator> &Creators()
{
    static std::unordered_map<uint32_t, MessageFactory::Creator> m = {
        {(uint32_t)MType::REQ_RPC, [] { return std::make_shared<RpcRequest>(); }},
        {(uint32_t)MType::RSP_RPC, [] { return std::make_shared<RpcResponse>(); }},
    };
    return m;
}

void MessageFactory::Register(MType mtype, const Creator &creator)
{
    Creators()[(uint32_t)mtype] = creator;
}

Message::ptr MessageFactory::Create(uint32_t mtype)
{
    auto &m = Creators();
    auto it = m.find(mtype);
    return it == m.end() ? nullptr : it->second();
}

}
