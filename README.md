# consumer-group-coordinator

一套本地多进程消费者组协调能力的参考实现：成员注册、心跳续期、会话过期、
两阶段再均衡（有时间上界、过程可观察）、基于代数与**实例身份**的栅栏、
按代数校验的位点提交，以及可崩溃恢复的位点持久化。既可在单个 JVM 内嵌使用，
也可让一个独立协调器进程服务多个真正独立的消费者进程。

## 模块结构

| 类 | 职责 |
| --- | --- |
| `GroupCoordinator` | 组协调器：成员管理、心跳、两阶段再均衡、代数栅栏、实例身份栅栏、提交校验、归属审计 |
| `OffsetCommitStore` | 追加式位点日志（CRC 帧 + fsync），崩溃后截断撕裂尾部 |
| `RangeAssignor` | 确定性范围分配器，保证完整覆盖且互不重叠 |
| `ConsumerMember` | 单进程内嵌的模拟消费者：心跳循环、位点提交、被栅栏后重新入组、崩溃/重启 |
| `CoordinatorConfig` | 分区数、心跳/会话/再均衡参数；非法配置在构造时即被拒绝 |
| `server.CoordinatorServer` / `process.CoordinatorServerMain` | 把协调器通过本地 TCP 暴露为一个独立进程 |
| `client.CoordinatorClient` / `process.ConsumerProcessMain` | 作为独立 OS 进程运行的真实消费者：两阶段入组、独立心跳连接、提交、重连、被顶替即退出 |
| `JoinResult` / `JoinStatus` | 两阶段协议结果：`STABLE` / `IN_PROGRESS`、当前代数、实例 token、是否超时收场 |

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
| `FENCED_INSTANCE` | 调用方的实例身份（token）已失效：被同 id 的更新实例顶替，或因超时/会话过期被剔除。该请求被永久拒绝，旧实例不得重试 |

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
- `rebalanceTimeoutMs >= 2 * heartbeatIntervalMs`（**再均衡上界独立于会话超时**，
  可以把再均衡上界调得很小而保持很长的会话）
- `topic` 非空白

参数确定后行为可复现：单进程协议测试使用可注入时钟（`MutableClock`）手动
推进时间，会话过期与再均衡超时判定完全确定；多进程测试使用真实墙钟但留有
充足余量，本地重复运行结果稳定。

## 多进程形态

除“同 JVM 内嵌”用法外，协调器可以作为一个独立进程对外服务，多个真正独立的
OS 进程作为消费者连上来。所有协调不变量（完整覆盖、互不重叠、代数/实例栅栏、
位点单调与持久化）在跨进程时与内嵌时完全一致——状态全部在协调器进程内、在同
一把锁下变更，客户端只是通过 TCP 发送同样的请求。

### 启动协调器进程

```bash
java -cp target/classes com.github.lechandonga.cgc.process.CoordinatorServerMain \
  <port(0=临时端口)> <partitionCount> <heartbeatIntervalMs> <sessionTimeoutMs> \
  <rebalanceTimeoutMs> <offsetStorePath> [topic]
# 启动后第一行输出：BOUND <实际端口>，便于发现临时端口
```

### 启动消费者进程

```bash
java -cp target/classes com.github.lechandonga.cgc.process.ConsumerProcessMain \
  127.0.0.1 <port> <memberId> [instanceToken|-] \
  [--duration-ms N] [--poll-ms N] [--slow-join] [--observe-rebalance] [--join-delay-ms N]
```

- 不带 token（或 `-`）即以**全新实例**身份加入该 member id；进程启动后会打印
  `TOKEN <token>`，之后所有请求都携带它。
- 关键事件逐行打印、立即 flush：`TOKEN / JOINED / ASSIGNED / COMMITTED /
  REBALANCING / TIMEOUT-CLOSED / REJECTED <op> <REASON> / FENCED`。
- 心跳走**独立连接**，提交再慢也不会队头阻塞会话续约。
- `--slow-join`：只完成两阶段入组的第 1 阶段即空转（用于演示/测试再均衡上界）。
- `--observe-rebalance`：再均衡进行中只观察/续约、不发起第 1 阶段，使窗口只能
  到设定上界才收场（用于确定性地观察超时）。
- `--join-delay-ms`：第 1、2 阶段之间故意停顿 N 毫秒，让并发加入的成员来得及
  取代本成员的窗口（用于确定性地复现“变化交错”，见下节）。

一个最小的手工验证（两个消费者进程共享 4 个分区）：

```bash
mvn -q compile
java -cp target/classes com.github.lechandonga.cgc.process.CoordinatorServerMain \
  0 4 100 60000 5000 /tmp/cgc-offsets.log &        # 记下 BOUND <port>
java -cp target/classes com.github.lechandonga.cgc.process.ConsumerProcessMain 127.0.0.1 <port> a &
sleep 2
java -cp target/classes com.github.lechandonga.cgc.process.ConsumerProcessMain 127.0.0.1 <port> b
# 稳定后可在各自输出看到：a  ASSIGNED a p0,p1   与   b  ASSIGNED b p2,p3
```

### 两阶段入组与“进行中”的可观察语义

多进程协议把一次成员变化建模为一个**有时间上界的两阶段窗口**：

1. 成员变化（加入）打开窗口、推进代数、立即栅栏旧代数所有归属；
2. 第 1 阶段 `JOIN`：成员声明参与本代数；窗口期间状态为 `REBALANCING`；
3. 第 2 阶段 `SYNC`：成员取得本代数分配。窗口期间得到的是
   `JoinStatus.IN_PROGRESS`——这是一个**可观察、可重试的中间态**，与最终失败
   （带 `RejectionReason` 的 `CoordinatorException`）明确区分，不会被当成普通
   失败，也不会永远观察不到结果；
4. 当所有应参与成员都完成第 2 阶段，窗口提前关闭；否则到
   `rebalanceTimeoutMs` **确定性收场**：
   - 完成第 2 阶段、或窗口期间持续心跳续约的成员保留；
   - 只发了第 1 阶段后失联、以及彻底无心跳的成员被剔除；
   - 等待方拿到 `STABLE` 且 `completedByTimeout=true`，结论明确、可复现。

`rebalanceTimeoutMs` 取不同值会带来确定的行为差异：参数改小，窗口更早收场；
改大，调用方能在更长时间内持续观察到 `IN_PROGRESS`。单测
`timeoutValueChangesBehavior` 用注入时钟对同一场景取两个上界值直接断言这一点。

### 成员同时加入与变化交错

多个成员几乎同时启动、或一个成员的变化撞上另一个成员的变化时，每一次新
变化都会**取代**此前尚未关闭的窗口（代数前进、旧窗口作废）。此时处在较早
那一轮变化里的成员，随后用来确认并领取归属的第 2 阶段请求，携带的正是它
自己第 1 阶段被告知等待的代数。协调器把这种情况识别为**“成员变化还在进行
中”**，而不是最终失败：

- 该成员确认的是“自己被告知等待、但已被取代”的代数（即它当前所知的最新
  代数）时，`SYNC` 返回 `IN_PROGRESS` 并指向当前仍在开放的窗口——调用方
  跟随该代数继续重试即可，不会崩溃；这次请求同时计为一次**存活续约**，
  因此持续确认的成员不会在窗口到点上界时被误剔除；
- 确认一个**自己从未参与过**的代数，仍是最终失败 `STALE_GENERATION`
  （“落后但合法”与“混乱请求”据此区分）；声称未来代数仍是
  `ILLEGAL_GENERATION`；
- 被顶替/已失效的旧实例**永远**先得到 `FENCED_INSTANCE`（身份检查先于一切
  状态/代数判断），无法顺着“进行中”的路径重新拿到分区或推进位点；同一
  id 的新实例走正常接管流程干净接手。

消费者进程端对称地处理交错：`IN_PROGRESS` 响应里携带的代数可能比自己刚问
的更新，消费者**跟随协调器指向的代数**继续第 2 阶段；遇到可重试的拒绝
（`STALE/ILLEGAL_GENERATION`、`REBALANCE_IN_PROGRESS`）时重新发第 1 阶段而
不是退出；只有 `FENCED_INSTANCE` 才让进程停止（退出码 3）。因此无论变化怎么
交错，活着且正常参与的成员最终都能拿到本轮归属并继续提交，到
`rebalanceTimeoutMs` 上界必有确定结论。

本地复现（真实多进程、无需外部服务）：测试
`MultiProcessTest.simultaneousStartupAndInterleavedJoinsConvergeWithoutProcessFailures`
先让三个消费者进程背靠背同时启动，再让成员 d 用 `--join-delay-ms 3000`
（第 1、2 阶段之间故意停顿）制造确定性交错——d 的窗口开放期间成员 e 加入并
取代 d 的代数，d 的首次确认必然落在已被取代的窗口上。断言：d 观察到的是
`REBALANCING`（进行中）而不是异常退出，最终五个进程的归属完整覆盖、互不重
叠、在同一代数上持续提交，且任何进程输出中都没有异常或 `FENCED`。配套单测
（注入时钟、完全确定）：`syncForSupersededWindowIsInProgressNotFinalFailure`、
`activelySyncingMemberSurvivesTimeoutCloseOfSupersededWindow`、
`fencedInstanceCannotRideInProgressSync`。

### 实例身份与“换了实例”的可判定

每个多进程成员在入组时获得一个不透明 **实例 token**；心跳、提交、离开都必须
携带当前 token。协调器因此能区分“成员 id”与“成员的某个实例”：

- 某成员崩溃/重启后以**新实例**（不带 token）重新加入同一 member id：协调器
  签发新 token，旧实例身份立即失效；
- 旧实例即使还在另一个进程里**持续心跳或提交**，也会被明确拒绝为
  `FENCED_INSTANCE`——无论它声称哪一代、无论组当前是否稳定；它既不能继续占用
  分区，也不能推进/覆盖位点，消费者进程收到后打印 `FENCED` 并以退出码 3 终止；
- 新实例干净接管该身份对应职责，并从已确认位点继续。

“被更新实例顶替”（永久 fence、旧实例必须退出）与“错过一次再均衡窗口但一直
活着”（窗口内有续约则保留、超时关闭后继续工作）据此可区分。

### 与既有单进程能力的兼容范围

- 原内嵌 API（`join(String)` / `heartbeat(id,gen)` / `commit(id,gen,...)`）与
  `ConsumerMember` 保持不变，原有单进程测试全部保留且不回归；其再均衡仍在锁内
  同步完成。
- 新的多进程 API（`joinGroup / syncGroup / syncCurrent` 及带实例 token 的
  heartbeat/commit/leave）建立在同一状态机之上；两种用法不会在一个组里混用
  （内嵌成员不携带实例身份，实例协议请求不会作用于它）。
- 位点日志格式、CRC/fsync 与崩溃恢复语义不变，协调器进程重启后从同一文件恢复。

## 验证方法

运行全部测试（单进程、真实 TCP、真实多进程均包含，无外部服务/凭据依赖）：

```bash
mvn test
```

测试覆盖（50 个用例）：

| 测试类 | 覆盖场景 |
| --- | --- |
| `CoordinatorConfigTest` | 各类非法配置被明确拒绝；再均衡上界可独立于会话超时调小 |
| `OffsetCommitStoreTest` | 提交持久化、重启恢复、撕裂尾部截断、CRC 损坏丢弃、位点倒退拒绝 |
| `GroupCoordinatorTest` | 加入/退出/过期触发再均衡、分配完整且不重叠、各类代数栅栏拒绝、归属区间审计 |
| `RebalanceProtocolTest` | 两阶段入组、`IN_PROGRESS` 与最终失败可区分、上界到期确定性收场（含超时标志）、参数取不同值行为不同、被顶替/超时剔除的旧实例 `FENCED_INSTANCE`、新实例干净接管；变化交错时确认被取代窗口得到 `IN_PROGRESS` 而非最终失败、持续确认的成员挺过超时收场、旧实例无法顺着“进行中”回场（注入时钟，完全确定） |
| `server.TcpProtocolIntegrationTest` | 同 JVM 内真实 loopback TCP：多连接共享分区、归属不重叠、越权提交拒绝、接管、旧连接被 fence、超时关闭 |
| `process.MultiProcessTest` | **独立 OS 进程**：多进程加入/提交/完整覆盖；某进程被 kill 后幸存者接走其分区并继续；新实例接管、旧进程持续心跳/提交被 `FENCED_INSTANCE` 拒绝并以码 3 退出；慢成员拖到上界、观察者先见 `REBALANCING` 后见 `TIMEOUT-CLOSED`；多进程同时启动与跨成员变化交错时无人异常退出、最终归属完整不重叠且全员继续提交 |
| `CrashRecoveryTest` | 成员崩溃→会话过期→同 id 重启后从已确认位点恢复；幸存成员持续接管全部分区 |
| `ConcurrencyTest` | 并发提交 × 再均衡 × 崩溃重启风暴（固定随机种子可复现）：位点单调、归属无重叠、无丢失更新 |

关键不变量（测试断言）：

1. 再均衡后各成员分区集合完整覆盖全部分区且互不重叠，跨进程亦然；
2. `verifyNoOwnershipOverlap()` 恒为真——任何时刻不存在两个有效归属；
3. 成员变化进行中只能观察到 `IN_PROGRESS`（可重试），收场有确定结论与上界；
   确认被交错取代的窗口同样是 `IN_PROGRESS`，只有确认从未参与的代数才是
   最终失败；
4. 被顶替/失效的旧实例在任意代数、任意状态下都得到 `FENCED_INSTANCE`，无法
   占用分区或推进位点；新实例可干净接管；
5. 已确认位点单调不减，任何成员的本地进度不超过已确认位点；
6. 崩溃重启后恢复的位点等于崩溃前最后一次确认的提交。

