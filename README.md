# consumer-group-coordinator

一套本地多进程消费者组协调能力的参考实现：成员注册、心跳续期、会话过期、
基于代数的再均衡与提交栅栏，以及可崩溃恢复的位点持久化。

## 模块结构

| 类 | 职责 |
| --- | --- |
| `GroupCoordinator` | 组协调器：成员管理、心跳、再均衡、代数栅栏、提交校验、归属审计 |
| `OffsetCommitStore` | 追加式位点日志（CRC 帧 + fsync），崩溃后截断撕裂尾部 |
| `RangeAssignor` | 确定性范围分配器，保证完整覆盖且互不重叠 |
| `ConsumerMember` | 模拟消费进程：心跳循环、位点提交、被栅栏后重新入组、崩溃/重启 |
| `CoordinatorConfig` | 分区数、心跳/会话/再均衡参数；非法配置在构造时即被拒绝 |

## 协调状态机

```
            join(首个成员)                join/leave/expire
  (启动) ──────────────▶ REBALANCING ◀──────────────── STABLE
                              │                          ▲
                              │ 分配计算完成（同步、持锁）  │
                              └──────────────────────────┘
                              最后一个成员离开 → EMPTY
```

- 再均衡在协调器锁内**同步完成**：代际（generation）递增、旧代际全部分区
  归属立即失效（栅栏）、新分配原子生效。不存在"半个分配"对外可见的窗口。
- 每次成员变化（加入 / 主动退出 / 会话过期）都会推进代际并触发再均衡。
- 协调器记录每个分区的归属区间历史（owner, generation, startMs, endMs），
  `verifyNoOwnershipOverlap()` 可审计"同一分区同一时刻只有一个有效归属"
  这一不变量；再均衡中旧归属先关闭、新归属后开启，重复归属窗口为零且可检测。

## 代数栅栏与拒绝原因

所有携带代际的请求（心跳、位点提交）都会被校验，拒绝原因可机器区分
（`CoordinatorException#reason()`）：

| 原因 | 含义 |
| --- | --- |
| `UNKNOWN_MEMBER` | 成员未注册（从未加入或已过期/退出） |
| `STALE_GENERATION` | 请求携带的代际落后于当前代际（被栅栏） |
| `ILLEGAL_GENERATION` | 请求携带了尚不存在的未来代际 |
| `REBALANCE_IN_PROGRESS` | 再均衡计算中，需重试 |
| `NOT_OWNER` | 当前代际中该分区不属于此成员 |
| `OFFSET_REGRESSION` | 提交位点低于已确认位点（倒退） |

被拒绝的请求不会改变任何已确立的归属或位点状态（校验先于写入，全部在
同一把锁内完成）。

## 位点语义

- **持久化**：每次提交追加一条 `[len][crc32][payload]` 帧并 `fsync`，
  fsync 返回后提交才被确认。
- **崩溃恢复**：恢复时顺序扫描帧，遇到不完整或 CRC 不匹配的尾部
  （崩溃时的半写入）即截断，恢复状态恒等于最后一次完整确认的提交，
  不会产生不可解析或倒退的位点。
- **单调性**：同一分区的提交位点不允许倒退（`OFFSET_REGRESSION`），
  崩溃恢复后从已确认位点继续，不会重复消费已确认的进度。
- **防丢失更新**：提交校验（成员、代际、归属、单调性）与落盘在协调器
  锁内原子完成；过期代际或越权成员的并发提交被栅栏拒绝，不会写入他人
  分区，也不会覆盖他人已确认的位点。

## 配置

`CoordinatorConfig` 参数：`topic`、`partitionCount`、`heartbeatIntervalMs`、
`sessionTimeoutMs`、`rebalanceTimeoutMs`、`offsetStorePath`。

非法配置在构造时抛出 `IllegalArgumentException`，绝不静默降级：

- `partitionCount >= 1`
- `heartbeatIntervalMs >= 1`
- `sessionTimeoutMs >= 2 * heartbeatIntervalMs`
- `rebalanceTimeoutMs >= sessionTimeoutMs`
- `topic` 非空白

参数确定后行为可复现：测试使用可注入时钟（`MutableClock`）手动推进时间，
会话过期判定完全确定。

## 验证方法

运行全部测试：

```bash
mvn test
```

测试覆盖（26 个用例）：

| 测试类 | 覆盖场景 |
| --- | --- |
| `CoordinatorConfigTest` | 各类非法配置被明确拒绝 |
| `OffsetCommitStoreTest` | 提交持久化、重启恢复、撕裂尾部截断、CRC 损坏丢弃、位点倒退拒绝 |
| `GroupCoordinatorTest` | 加入/退出/过期触发再均衡、分配完整且不重叠、各类代数栅栏拒绝、归属区间审计 |
| `CrashRecoveryTest` | 成员崩溃→会话过期→同 id 重启后从已确认位点恢复；幸存成员持续接管全部分区 |
| `ConcurrencyTest` | 并发提交 × 再均衡 × 崩溃重启风暴（固定随机种子可复现）：位点单调、归属无重叠、无丢失更新；提交与再均衡确定性交错下状态不出现半更新 |

关键不变量（测试断言）：

1. 再均衡后各成员分区集合完整覆盖全部分区且互不重叠；
2. `verifyNoOwnershipOverlap()` 恒为真——任何时刻不存在两个有效归属；
3. 已确认位点单调不减，任何成员的本地进度不超过已确认位点；
4. 崩溃重启后恢复的位点等于崩溃前最后一次确认的提交。
