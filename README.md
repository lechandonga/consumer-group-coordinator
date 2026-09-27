# consumer-group-coordinator

一套本地多进程消费者组协调能力的参考实现：成员注册、心跳续期、会话过期、
基于代数与实例纪元的双重栅栏、有时间上界的再均衡，以及可崩溃恢复的位点
持久化。支持两种部署形态：单进程内嵌，或"协调器进程 + 若干消费者进程"
的多进程组网。

## 模块结构

| 类 | 职责 |
| --- | --- |
| `GroupCoordinator` | 组协调器：成员管理、心跳、再均衡、代数+实例纪元栅栏、提交校验、归属审计 |
| `Coordinator` | 协调 API 接口，内嵌协调器与远程客户端实现同一接口 |
| `CoordinatorServer` / `CoordinatorClient` | TCP 行式协议的服务端与客户端，让独立进程参与同一消费者组 |
| `CoordinatorServerMain` / `ConsumerMemberMain` | 可独立启动的协调器进程与消费者进程入口 |
| `OffsetCommitStore` | 追加式位点日志（CRC 帧 + fsync），崩溃后截断撕裂尾部 |
| `RangeAssignor` | 确定性范围分配器，保证完整覆盖且互不重叠 |
| `ConsumerMember` | 消费者成员：心跳循环、位点提交、被栅栏后重新入组、被顶替即停、崩溃/重启 |
| `CoordinatorConfig` | 分区数、心跳/会话/再均衡参数、再均衡模式；非法配置在构造时即被拒绝 |

## 协调状态机

```
            join(首个成员)                join/leave/expire
  (启动) ──────────────▶ REBALANCING ◀──────────────── STABLE
                              │                          ▲
                              │ 分配计算完成               │
                              └──────────────────────────┘
                              最后一个成员离开 → EMPTY
```

再均衡有两种模式（`CoordinatorConfig.rebalanceMode()`）：

- **IMMEDIATE（默认，兼容既有行为）**：再均衡在协调器锁内同步完成，代际递增、
  旧归属立即栅栏、新分配原子生效，不存在对外可见的中间窗口。
- **BARRIER**：成员变化先栅栏旧代际并打开一个**确认屏障**（期间无任何分区
  归属），所有存活成员须在 `rebalanceTimeoutMs` 内重新入组确认；全员确认即
  建立新分配。到达上界仍未收齐时，协调方**确定性地**驱逐未确认成员并以已确认
  成员完成分配——不会无限期拖延。屏障期间心跳/提交得到可机器区分的
  `REBALANCE_IN_PROGRESS`（进行中结果，区别于各类终态失败）；`joinAndAwait`
  的等待方在屏障收场后拿到确定结论（最终分配），等待时间上界即屏障截止期。
  `rebalanceTimeoutMs` 取不同值会带来确定性的行为差异（见
  `RebalanceBarrierTest.rebalanceTimeoutParameterTakesEffect`）。

两种模式下协调器都记录每个分区的归属区间历史，
`verifyNoOwnershipOverlap()` 可审计"同一分区同一时刻只有一个有效归属"。

## 代数栅栏与实例纪元栅栏

- **代数（generation）**：每次成员变化推进，携带旧代际的请求被拒绝。
- **实例纪元（memberEpoch）**：每次 `join` 为成员签发新的纪元。同一成员 id
  以新实例重新加入后，旧实例即使仍在发心跳/提交，也会被
  `FENCED_MEMBER_EPOCH` 明确拒绝——不能继续占用分区，也不能推进或覆盖别人
  的进度；新实例无需再均衡即可接管原身份的归属。`ConsumerMember` 观察到
  该拒绝即终止自身（`isFenced()`），不会与顶替者反复争抢。

拒绝原因均可机器区分（`CoordinatorException#reason()`）：

| 原因 | 含义 |
| --- | --- |
| `UNKNOWN_MEMBER` | 成员未注册（从未加入或已过期/退出） |
| `STALE_GENERATION` | 请求携带的代际落后于当前代际（被栅栏） |
| `ILLEGAL_GENERATION` | 请求携带了尚不存在的未来代际 |
| `REBALANCE_IN_PROGRESS` | 再均衡屏障未收场（进行中，非终态失败），需重新入组确认 |
| `FENCED_MEMBER_EPOCH` | 该成员实例已被同 id 的新实例顶替，必须停止 |
| `NOT_OWNER` | 当前代际中该分区不属于此成员 |
| `OFFSET_REGRESSION` | 提交位点低于已确认位点（倒退） |

被拒绝的请求不会改变任何已确立的归属或位点状态（校验先于写入，全部在
同一把锁内完成）。

## 位点语义

- **持久化**：每次提交追加一条 `[len][crc32][payload]` 帧并 `fsync`，
  fsync 返回后提交才被确认。
- **崩溃恢复**：恢复时顺序扫描帧，遇到不完整或 CRC 不匹配的尾部即截断，
  恢复状态恒等于最后一次完整确认的提交。
- **单调性**：同一分区的提交位点不允许倒退，崩溃恢复后从已确认位点继续。
- **防丢失更新**：提交校验（成员、实例纪元、代际、归属、单调性）与落盘在
  协调器锁内原子完成。

## 多进程使用方式

启动协调器进程（打印 `LISTENING <port>` 后对外服务）：

```bash
mvn compile
java -cp target/classes com.github.lechandonga.cgc.CoordinatorServerMain \
    --port 9000 --partitions 4 --store /tmp/cg-offsets.log \
    --heartbeat-ms 100 --session-ms 1000 --rebalance-ms 2000 --mode BARRIER
```

在另外的终端各启动一个消费者进程（`--id` 即成员身份）：

```bash
java -cp target/classes com.github.lechandonga.cgc.ConsumerMemberMain \
    --host 127.0.0.1 --port 9000 --id m1
```

消费者进程在 stdout 输出可解析事件：`READY` / `ASSIGNED`（归属变化）/
`POSITIONS`（位点推进）/ `FENCED`（被同 id 新实例顶替，进程随即退出）/
`STOPPED`。同 id 再启动一个消费者进程即可观察"旧实例被栅栏、新实例接管"。

进程间协议为 TCP 行式文本协议（见 `CoordinatorServer` 类注释），拒绝原因
与内嵌 API 完全一致；`STATE`/`VERIFY` 管理请求可观察组状态与归属审计结果。
应用代码面向 `Coordinator` 接口编程即可在两种部署形态间切换，
`ConsumerMember` 不需要任何改动。

## 兼容范围

- 既有的单进程内嵌用法完全保留：`GroupCoordinator` 的
  `join/heartbeat/commit/leave/fetchCommitted` 旧签名行为不变（无纪元的
  旧式重载解析为成员当前纪元），默认 `IMMEDIATE` 模式语义与此前一致，
  既有 26 个用例不回归。
- `CoordinatorConfig` 保留 6 参构造（默认 `IMMEDIATE`）；`rebalanceTimeoutMs`
  在 `IMMEDIATE` 模式下无等待窗口可约束，在 `BARRIER` 模式下是屏障的硬性上界。
- 新增能力（实例纪元栅栏、屏障模式、网络层）均为增量，不改变既有默认行为。

## 配置

`CoordinatorConfig` 参数：`topic`、`partitionCount`、`heartbeatIntervalMs`、
`sessionTimeoutMs`、`rebalanceTimeoutMs`、`offsetStorePath`、`rebalanceMode`。

非法配置在构造时抛出 `IllegalArgumentException`，绝不静默降级：

- `partitionCount >= 1`
- `heartbeatIntervalMs >= 1`
- `sessionTimeoutMs >= 2 * heartbeatIntervalMs`
- `rebalanceTimeoutMs >= sessionTimeoutMs`
- `topic` 非空白

参数确定后行为可复现：单进程测试使用可注入时钟（`MutableClock`）手动推进
时间，会话过期与屏障超时判定完全确定。

## 验证方法

运行全部测试（纯本地，不依赖任何外部服务、凭据或付费资源）：

```bash
mvn test
```

测试覆盖（40 个用例）：

| 测试类 | 覆盖场景 |
| --- | --- |
| `CoordinatorConfigTest` | 各类非法配置被明确拒绝 |
| `OffsetCommitStoreTest` | 提交持久化、重启恢复、撕裂尾部截断、CRC 损坏丢弃、位点倒退拒绝 |
| `GroupCoordinatorTest` | 加入/退出/过期触发再均衡、分配完整且不重叠、各类代数栅栏拒绝、归属区间审计 |
| `MemberEpochFencingTest` | 同 id 新实例顶替：旧实例心跳/提交被 `FENCED_MEMBER_EPOCH` 拒绝、无法覆盖新实例进度、被顶替成员自行停止 |
| `RebalanceBarrierTest` | 屏障期间请求得到可区分的进行中结果、全员确认即完成、超时确定性驱逐未确认成员、`rebalanceTimeoutMs` 取值变化带来确定性行为差异、`joinAndAwait` 等待方拿到确定结论 |
| `NetworkedCoordinatorTest` | 回环 TCP 下的加入、屏障确认、提交、跨成员与实例纪元栅栏 |
| `MultiProcessTest` | 真实独立 JVM 进程：多进程完整且不重叠覆盖、崩溃进程分区被幸存进程接管并继续推进、跨进程实例顶替栅栏 |
| `CrashRecoveryTest` | 成员崩溃→会话过期→同 id 重启后从已确认位点恢复；幸存成员持续接管全部分区 |
| `ConcurrencyTest` | 并发提交 × 再均衡 × 崩溃重启风暴（固定随机种子可复现）：位点单调、归属无重叠、无丢失更新 |

关键不变量（测试断言）：

1. 再均衡收场后各成员分区集合完整覆盖全部分区且互不重叠；
2. `verifyNoOwnershipOverlap()` 恒为真——任何时刻不存在两个有效归属；
3. 已确认位点单调不减，任何成员的本地进度不超过已确认位点；
4. 崩溃重启后恢复的位点等于崩溃前最后一次确认的提交；
5. 被顶替的旧实例请求被明确拒绝，新实例干净接管原身份职责；
6. 成员变化在 `rebalanceTimeoutMs` 内必有确定收场，进行中结果与终态失败可区分。
