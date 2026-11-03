#include "entry/consumer.hpp"
#include <algorithm>

namespace nexus {

Consumer::Consumer(const std::string &reg_ip, uint16_t reg_port)
    : _reg(reg_ip, reg_port)
{
}

Consumer::~Consumer()
{
    Stop();
}

bool Consumer::Subscribe(const std::string &service)
{
    _service = service;
    _reg.SetOnPush([this](const std::string &svc, const std::string &inst, bool added)
                   { OnPush(svc, inst, added); });
    if (!_reg.Connect())
        return false;
    std::vector<std::string> snap;
    if (!_reg.List(service, snap) || !_reg.Watch(service))
        return false;
    std::lock_guard<std::mutex> lk(_mutex);
    // 快照必须去重：Watch 的快照 push 可能已抢在 List 结果入表前被 OnPush 加过，
    // 重复名 REMOVE 一次杀不死
    for (auto &s : snap)
        if (std::find(_names.begin(), _names.end(), s) == _names.end())
            _names.push_back(s);
    return true;
}

// 推送回调跑在 reg 的 net 线程：只改表。RpcClient 的建连/销毁（都带阻塞）
// 一律挪到 Call 的业务线程做，net 线程不许长阻塞
void Consumer::OnPush(const std::string &service, const std::string &instance, bool added)
{
    if (service != _service)
        return;
    std::lock_guard<std::mutex> lk(_mutex);
    auto it = std::find(_names.begin(), _names.end(), instance);
    if (added && it == _names.end())
        _names.push_back(instance);
    if (!added && it != _names.end())
        _names.erase(it);
}

bool Consumer::Call(const std::string &method, const Json::Value &params,
                    RCode &code, Json::Value &result)
{
    std::shared_ptr<rpc::RpcClient> cli;
    std::string inst;
    bool fresh = false;
    std::vector<std::shared_ptr<rpc::RpcClient>> dropping;
    {
        std::lock_guard<std::mutex> lk(_mutex);
        // 名单里没有的实例，客户端一起摘牌
        for (auto pit = _pool.begin(); pit != _pool.end();)
            if (std::find(_names.begin(), _names.end(), pit->first) == _names.end())
            {
                dropping.push_back(std::move(pit->second.cli));
                pit = _pool.erase(pit);
            }
            else
                ++pit;
        size_t n = _names.size();
        for (size_t k = 0; k < n; k++) // 轮询最多绕全表一圈
        {
            inst = _names[_rr++ % n];
            auto pit = _pool.find(inst);
            if (pit == _pool.end())
            {
                size_t c = inst.find(':');
                Entry e;
                e.cli = std::make_shared<rpc::RpcClient>(
                    inst.substr(0, c), (uint16_t)stoi(inst.substr(c + 1)));
                cli = e.cli;
                _pool[inst] = std::move(e); // ready=false：并发的绕到者自动换人
                fresh = true;
                break;
            }
            if (pit->second.ready)
            {
                cli = pit->second.cli;
                break;
            }
        }
    }
    for (auto &d : dropping)
        d->Stop();
    if (!cli)
        return false;
    if (fresh)
    {
        if (!cli->Connect())
        {
            std::lock_guard<std::mutex> lk(_mutex);
            _pool.erase(inst);
            return false;
        }
        std::lock_guard<std::mutex> lk(_mutex);
        auto pit = _pool.find(inst);
        if (pit == _pool.end() || pit->second.cli != cli)
            return false; // 连的期间被摘牌：cli 出作用域自灭
        pit->second.ready = true;
    }
    if (!cli->Call(method, params, code, result))
    {
        {
            std::lock_guard<std::mutex> lk(_mutex);
            auto pit = _pool.find(inst);
            if (pit != _pool.end() && pit->second.cli == cli)
                _pool.erase(inst); // 断线实例就地摘牌；名单侧等 registry push 确认
        }
        cli->Stop(); // 锁外干：Stop 带 join
        return false;
    }
    return true;
}

void Consumer::Stop()
{
    _reg.Stop();
    std::vector<std::shared_ptr<rpc::RpcClient>> all;
    {
        std::lock_guard<std::mutex> lk(_mutex);
        for (auto &[inst, e] : _pool)
            all.push_back(e.cli);
        _pool.clear();
        _names.clear();
    }
    for (auto &c : all)
        c->Stop();
}

}
