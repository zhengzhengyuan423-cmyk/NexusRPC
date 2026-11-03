#pragma once
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include "protocol/pump.hpp"
#include "registry/registry_message.hpp"

namespace nexus {

class RegistryServer
{
public:
    explicit RegistryServer(int port, NetOptions opts = {});

    void Start(); // 阻塞，直到 Stop 触发收摊
    void Stop();

private:
    struct Entry
    {
        std::string service;
        uint64_t conn = 0; // 谁登记的：断线即驱逐
    };

    bool OnFrame(uint64_t conn_id, uint32_t mtype, const std::string &rid, const std::string &body);
    void Handle(uint64_t conn_id, const std::string &rid, RegistryRequest::ptr &req);
    void Respond(uint64_t conn_id, const std::string &rid, RCode code, const Json::Value &data);
    void Push(const std::string &service, const std::string &instance, bool added); // 调用方须持 _mutex
    void OnClosed(uint64_t conn_id);

    NetServer _net;
    std::unique_ptr<FramePump> _pump;
    std::mutex _mutex;
    std::unordered_map<std::string, Entry> _leases;                         // instance → 登记
    std::unordered_map<std::string, std::unordered_set<uint64_t>> _watchers; // service → conn 集
};

}
