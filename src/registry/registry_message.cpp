#include "registry/registry_message.hpp"

namespace nexus {

bool RegistryRequest::Check()
{
    if (!_body[key::OP].isIntegral() || !_body[key::SERVICE].isString())
        return false;
    if (!_body[key::INSTANCE].isNull() && !_body[key::INSTANCE].isString())
        return false;
    return true;
}

bool RegistryResponse::Check()
{
    return _body[key::RCODE].isIntegral();
}

bool RegistryPush::Check()
{
    return _body[key::SERVICE].isString() && _body[key::INSTANCE].isString() &&
           _body[key::ADDED].isBool();
}

void EnsureRegistryMessages()
{
    static const bool inited = []
    {
        MessageFactory::Register(MType::REQ_REG, [] { return std::make_shared<RegistryRequest>(); });
        MessageFactory::Register(MType::RSP_REG, [] { return std::make_shared<RegistryResponse>(); });
        MessageFactory::Register(MType::PUSH_REG, [] { return std::make_shared<RegistryPush>(); });
        return true;
    }();
    (void)inited;
}

}
