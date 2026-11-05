# Nexus

C++17 实现的 reactor 网络库，上面架了 RPC、注册中心和流量治理（限流 / 熔断）。技术栈：C++17 / CMake / pthread / jsoncpp 

 52 个测试用例，全部通过。

```bash
cmake -S . -B build
cmake --build build -j
ctest --test-dir build
```

（外部依赖只有 `libjsoncpp-dev` 和 `libgtest-dev` 两个）

## 设计亮点

**事件驱动的骨架。** epoll 边缘触发，一个 baseloop 管 accept 和定时器，一组 io 线程分管连接，业务 handler 交给 biz 线程池。连 Ctrl-C 都走事件回路：信号先屏蔽，signalfd 变成文件事件，loop 自己把自己叫醒，不需要管道自唤醒那套。biz 队列有界，塞满就丢并计数，不把反压传回 io。

**手写的协议栈。** 4 字节长度头 + JSON 包体，rid 把请求和响应配平。FramePump 处理粘包拆包：字节进缓冲区，能成帧就成帧；帧头坏，连接就地处决。RPC 客户端三种形态共用同一套帧——阻塞式 Call、返回 `std::future` 的 Call、回调式 Call，in-flight 表按 rid 挂账，回包线程统一兑现。

**服务发现是推送驱动的。** consumer 订阅时 List 一次快照，之后 WATCH 订推送，实例的进出全靠 registry 主动下发，客户端没有轮询。provider 进程死掉，整条链自动走完：TCP FIN 到 registry 按连接驱逐，push REMOVE 到 consumer 摘名，连接池跟着拆连接。

**治理内核没有计时线程。** 限流和熔断的滑动窗口都是 LeapArray：两格 500ms，格子新不新只看时间戳，读数时在双检锁里惰性换格。熔断支持错误率和慢调用两种策略，半开探测的放行名额是一枚 CAS 票，抢到的那个去试探，其余等着。

**治理对 RPC 是可选依赖。** router 手里只有一枚 Sentinel 指针，不挂就全部直通；挂了，也只是在查表之后和 handler 前后各多两个调用点。registry、协议层对治理模块零依赖，整个项目里只有 rpc 和 entry 两处认识它。

**按方法名分链配置。** 同一个 server，Add 可以 qps=100、Pay 可以 qps=10，熔断阈值也分开判。配置表在 Start 后只读，找链这一步没有锁（链里的窗口和状态机各有各的小锁）。

## 目录结构

```
src/net/        reactor：EventLoop / Channel / Poller(epoll) / Connection /
                Acceptor / LoopThreadPool / 业务线程池
src/protocol/   LV 帧编解码、Message、Dispatcher、FramePump、Requestor
src/rpc/        RpcRouter（查表 + 治理闸门）、RpcServer / RpcClient（同步 / future / 回调）
src/registry/   RegistryServer、RegistryClient
src/entry/      Provider / Consumer：注册、心跳、连接池、轮询这些琐事收进一个入口
src/sentinel/   治理内核：LeapArray 滑动窗口、限流器、熔断器、按方法名的责任链
demo/           9 个可执行程序
tests/          11 个 GoogleTest 二进制，52 个用例
```

`src/sentinel/` 不依赖任何上层模块，直接编在 `nexus_rpc` 里。

## 线程和锁

每进程 1 个 baseloop 管 accept、定时器、信号，`NetOptions` 决定 io 线程数和 biz 线程数。biz 队列有界，放不下的请求丢掉并打日志，不反压 io 线程。

四把锁各管各的数据：router 方法表一把 shared_mutex（读多写少）；registry 表一把 `_mutex`（改表和 push 要在同一把锁里）；consumer 名单和连接池一把 `_mutex`，registry push 回调只改表，建连和析构这种会阻塞的活挪回业务线程干；熔断状态迁移一把小锁，只串行化开窗、拉半开，记账不排队。

关停顺序：biz 线程池先停、io 再停——worker 手头任务还要往外 Send；`Shutdown` 幂等；acceptor 关停会把 listen fd 真关掉，只摘 epoll 监控的话内核还在继续完成握手。

## 快速上手

### 1. RPC + 治理，单机直连

```cpp
rpc::RpcServer srv(19851);
srv.RegisterService("Add", {{"a", rpc::VType::INTEGRAL}, {"b", rpc::VType::INTEGRAL}},
                    rpc::VType::INTEGRAL, handler);
Sentinel::LimiterCfg lmt;  lmt.enabled = true;  lmt.qps = 100;
srv.ConfigureGovernance("Add", lmt);   // 要在 Start 之前登记
srv.Start();                           // 阻塞，直到 Stop
```

### 2. 注册中心 + 入口层

```bash
./build/demo/registry_server_main 19890   # 注册中心
./build/demo/provider_main                # 起服务，自登记，带心跳
./build/demo/consumer_main                # 订阅名单轮询直连，kill -9 一个 provider 看自动切流
```

```cpp
Provider p("calc", 19891, "127.0.0.1", 19890);
p.RegisterService(...);
p.ConfigureGovernance("Add", lmt);      // 服务端闸门
p.Start();

Consumer c("127.0.0.1", 19890);
c.Subscribe("calc");
Sentinel::LimiterCfg pace;  pace.enabled = true;  pace.qps = 1;  pace.queueing_ms = 3000;
c.ConfigureGovernance("Add", pace);     // 客户端排队在这里才有效
RCode code;  Json::Value r;
c.Call("Add", params, code, r);         // 被限流也返回 true，code 是 LIMITED
```

### 3. 治理三场景演示

```bash
./build/demo/gov_demo
```

场景 A 服务端 qps=2 连打 6 发，收到 2 个 OK、4 个 limited；场景 B 制造两笔违约让熔断开窗，第三次直接吃 limited，恢复窗口过后服务转好，半开探测通过，归闭；场景 C 调用方 qps=1 排队连拨 3 发，三发全部成功，实测耗时约 1ms、1013ms、990ms——等待发生在调用方自己身上。

## 压测

`rpc_bench` 是自带的压测工具，第一个参数选链路：direct 是客户端到服务端点对点；consumer 带着注册中心和 2 个 provider，每线程一个 Consumer，走完整服务层。

```bash
./build/demo/rpc_bench direct 10 45000 2 2000   # 服务端限流 2000/s，约 23 秒
./build/demo/rpc_bench consumer 10 4000 2 200   # 本地限流每线程 200/s，约 15 秒
```

Release 构建、本机实测：

| 场景 | 成功速率 | 单笔平均 |
|---|---|---|
| direct，无治理 | 18180 笔/秒 | 539µs |
| consumer，无治理 | 17323 笔/秒 | 534µs |
| direct，服务端 qps=2000（23.4 秒） | 2049 笔/秒 | 515µs |
| consumer，本地 qps=200×10（14.8 秒） | 2022 笔/秒 | 600µs |

走注册中心和服务层这条路，请求速率只比直连低 5% 左右。限流设 2000，测 20 秒以上，实际每秒通过 2040 左右，比设定值高 2~3%：限流器每 500 毫秒重新数一遍，跨在格子边界上的请求会被多放进去几个。另外，测试时间短的话，可能会受到滑动窗口精确性的影响。
