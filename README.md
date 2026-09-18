# Consumer Group Coordinator（消费者组协调器）

一套可在多个**本地进程**间运行的消费者组协调能力：在成员动态加入、主动退出或故障失效
的情况下，持续、可验证地推进分区消费；保证同一分区同一时刻只有一个有效归属，
位点崩溃可恢复，过期代数/化身的请求被可区分地拒绝且不改变已确立状态。

仅依赖 JDK（Java 21+）与 JUnit 5，无任何外部运行时依赖。

---

## 1. 模块结构

```
com.github.lechandonga.cgcoordinator
├── config/CoordinatorConfig   配置与启动期强校验（非法即拒绝，不静默降级）
├── assign/PartitionAssignor   分配器接口
│   └── RangeAssignor          确定性排序轮询分配（完整覆盖、互不重叠、可复现）
├── model/                     Assignment / Snapshot / CommitResult
├── reject/RejectReason        可区分的拒绝原因枚举
├── offset/OffsetStore         位点存储接口
│   └── FileOffsetStore        崩溃原子检查点（temp + fsync + 原子 rename + 校验和）
├── audit/OwnershipAuditLog    GRANT/REVOKE 归属事件与“同代重复归属”检测
├── group/GroupCoordinator     单锁状态机核心（围栏、再均衡、失效、提交校验）
├── net/Protocol               文本行协议
│   └── CoordinatorServer      TCP 服务端（每连接一线程 + reaper 线程）
├── client/ConsumerClient      本地进程消费者（join/心跳/围栏提交/位点恢复）
└── cli/Main                   coordinator / consumer 两种进程入口
```

---

## 2. 协调状态机

协调器侧每个组有三种状态：

```
        首个成员加入                 期望成员全部重连 或 收集窗口到期
 EMPTY ───────────────▶ REBALANCING ─────────────────────────────▶ STABLE
   ▲                        │  ▲                                        │
   │ 最后成员退出/失效       │  └──────────── 加入 / 退出 / 失效 ◀────────┘
   └────────────────────────┘
```

- **再均衡由固定“成员收集窗口”（`join-ms`）驱动**：进入 REBALANCING 时记录
  “期望成员集合”（当时的全体活跃成员）。窗口内只要所有期望成员（含窗口内新加入者）
  都重连即**提前完成**；窗口到期时仍未重连的成员一律移出本代。
- 该固定窗口使一簇近乎同时发生的加入由**恰好一次**再均衡处理，不依赖线程调度，
  行为确定可复现。
- `expireSessions`（由 reaper 周期调用）移除超过 `session-ms` 未心跳的成员并触发再均衡；
  `tickRebalance` 保证即使没有请求到达，收集窗口到期也会最终化。

### 唯一归属不变量（先撤销、后授予）

每次代际切换都在**同一把锁内**完成：

1. 对旧代数的每个分区发 `REVOKE` 并清空归属表；
2. 分配器对新成员集合产出新计划，校验“分区数完整、无重复”；
3. 提交新归属表后，对每个分区发 `GRANT`。

因此两个**已确立**的归属在任何时刻都不会重叠。再均衡期间可能存在一个极短的
“旧归属已撤销、新归属尚未授予”的**空窗**（这正是 fencing 点），而绝不是“两个属主并存”。
`OwnershipAuditLog.hasSimultaneousOverlap()` 会机器化检测同代双属主，测试中持续断言其恒为 `false`。

---

## 3. 代数（Generation）与化身（Incarnation）围栏

每个请求都携带三元组 `(memberId, incarnationId, generation)`：

| 情况 | 拒绝原因 `RejectReason` |
| --- | --- |
| 成员不存在 | `UNKNOWN_MEMBER` |
| 请求代数 ≠ 当前代数（含旧代心跳/提交/退出） | `STALE_GENERATION` |
| 旧进程的化身（崩溃重启后僵尸实例） | `FENCED_INCARNATION` |
| 再均衡进行中发心跳/提交 | `REBALANCING` |
| 给不属于自己的分区提交 | `NOT_OWNER` |
| 位点比已确认值更小 | `REGRESSION` |
| 分区越界 / 请求格式错误 | `UNKNOWN_PARTITION` / `BAD_REQUEST` |

- `memberId` 是跨重启稳定的客户端身份；每次进程启动生成新的 `incarnationId`。
  旧化身即使残留连接也会被 `FENCED_INCARNATION` 拒绝。
- **所有被拒绝的请求都不会改变已确立的归属或位点状态**（校验全部发生在任何状态写入之前）。
- 提交的额外校验顺序：分区合法 → 成员/化身合法 → 代数匹配 → 组处于 STABLE →
  调用方是该分区当前唯一属主 → 位点不回退 → 才落盘。

---

## 4. 位点语义与崩溃恢复

- 位点含义为“该分区下一个待消费偏移”，按分区独立维护，**单调非递减**。
- `FileOffsetStore` 以单个检查点文件保存全部分区：
  写入临时文件 → `fsync` 内容 → **原子 rename** 覆盖 →（尽力）fsync 目录。
  - 崩溃只会留下“旧的完整文件”或“新的完整文件”二者之一；半截临时文件永不被改名，
    启动时直接丢弃。
  - 文件头含版本 `epoch`，每条记录含校验和；任何截断、篡改、重复、越界、
    半数目不匹配都会在加载时**显式抛错**，而不是解析出倒退或不可解析状态。
- 进程崩溃重启后，消费者以同一 `memberId` 重新加入，并从 `FETCH` 得到的
  **最后已确认位点**恢复消费；半提交不产生可见影响。

---

## 5. 可配置项（非法配置立即拒绝）

| 参数 | 约束 |
| --- | --- |
| `--partitions` | `>= 1` |
| `--session-ms` | `>= 1` |
| `--heartbeat-ms` | `>= 1` 且 `<= session-ms` |
| `--join-ms` | `>= 1`（再均衡收集窗口/加入阻塞上限） |
| `--checkpoint-dir` | 非空白 |

确定可复现性来自：确定性分配器 + 固定收集窗口 + reaper 使用固定会话超时。

---

## 6. 多进程运行方式

先打包：

```bash
mvn -q package
JAR=target/consumer-group-coordinator-1.0-SNAPSHOT.jar
```

启动协调器进程：

```bash
java -cp $JAR com.github.lechandonga.cgcoordinator.cli.Main coordinator \
  --port 9000 --partitions 8 --session-ms 10000 \
  --heartbeat-ms 3000 --join-ms 2000 --checkpoint-dir ./data
```

在多个终端分别启动消费者进程（模拟多个本地进程）：

```bash
java -cp $JAR com.github.lechandonga.cgcoordinator.cli.Main consumer \
  --host 127.0.0.1 --port 9000 --member a --heartbeat-ms 3000 --messages 100
java -cp $JAR com.github.lechandonga.cgcoordinator.cli.Main consumer \
  --host 127.0.0.1 --port 9000 --member b --heartbeat-ms 3000 --messages 100
```

杀掉某个消费者（故障，不发 LEAVE），协调器会在会话超时后将其失效并再均衡；
用相同 `--member` 重启会以新化身接管并从最后位点恢复。

> 说明：示例 CLI 消费者在首次 join 后会等待一个 `join-ms` 收集窗口再 join 一次，
> 以便让启动时的成员加入洪峰收敛到同一稳定代后再消费。协议正确性可在
> “多个客户端在同一收集窗口内重连”时直接观察到——它们会被分配到同一代、
> 各拿不相交的分区（例如 6 分区 2 成员：`a=[0,2,4]`、`b=[1,3,5]`）。

### 线协议（便于观测）

请求/响应各一行，`|` 分隔：

```
JOIN|member|incarnation|timeoutMs        -> OK|generation|state|p1,p2,...
HB|member|incarnation|generation         -> OK|generation|state
LEAVE|member|incarnation|generation      -> OK|generation|state
COMMIT|member|incarnation|generation|p|o -> OK|generation|partition|offset
FETCH|partition                          -> OK|offset
SNAP                                     -> OK|generation|state|member:parts;...
任何拒绝                                  -> ERR|REASON|message
```

可用 `nc 127.0.0.1 9000` 手工发请求观察围栏与再均衡。

---

## 7. 如何验证

```bash
mvn test
```

测试覆盖：

- `CoordinatorConfigTest`：非法分区数/超时/心跳大于会话/空目录均被拒绝。
- `RangeAssignorTest`：完整覆盖、互不重叠、与输入顺序无关的确定性。
- `FileOffsetStoreTest`：恢复到已确认位点、倒退拒绝、等位点幂等、
  崩溃残留临时文件被丢弃、损坏检查点显式报错、分区越界拒绝。
- `OwnershipAuditLogTest`：跨代先撤后授无重叠、同代双属主可被检测。
- `GroupCoordinatorTest`：单/多成员加入、再均衡完整覆盖与唯一归属、
  旧代心跳/提交拒绝、僵尸化身围栏、非属主提交拒绝、主动退出、
  会话失效重均衡、被拒提交不改变归属。
- `CoordinatorServerIT`：真实回环 TCP 上的 join/心跳/提交流程，
  旧代提交 `STALE_GENERATION`、旧化身 `FENCED_INCARNATION`、坏命令不拖垮服务。
- `ConsumerGroupIT`（端到端，多客户端=多进程）：
  - 并发再均衡后 8 个分区恰好各属一个成员；
  - 成员故障（无 LEAVE 关闭）后被失效并再均衡，无分区丢失或双属主；
  - 旧代/非属主提交被拒且状态不变；
  - 崩溃后以相同 memberId 重启从最后位点恢复，倒退提交被 `REGRESSION` 拒绝；
  - 成员反复加入/退出与并发提交交错：接受的提交必来自当时唯一属主、
    位点单调不丢更新，审计日志全程无同代重复归属。

---

## 8. 关键并发模型

- 协调核心的所有读写都在一个监视器内串行化；TCP 层的多线程/多进程只是入口，
  最终全部汇聚到同一把锁，因此“进程内并发”与“多进程并发”语义一致。
- 归属表变更与位点提交分别保证：归属在代际切换时原子替换（先撤后授）；
  位点提交在 STABLE 且属主匹配时才落盘，检查点本身原子替换，杜绝半更新。
