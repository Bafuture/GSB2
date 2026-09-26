# Saga 分布式事务协调器

一个基于 **Saga 模式**（编排式 / orchestration）的分布式事务协调器，用于替代沉重的两阶段提交（2PC）。
Maven 多模块工程，仅依赖内嵌 H2 与 JUnit，无外部数据库、无框架依赖。

## 模块结构

| 模块 | 说明 |
|------|------|
| `participant` | 参与方屏障（`ParticipantBarrier`）：幂等、空回滚、悬挂三种防护，状态落 H2 |
| `coordinator` | 协调器核心：Java DSL、状态机、持久化、超时/重试、恢复、故障注入 |
| `example` | 跨「账户 / 库存 / 积分」三个参与方的演示与端到端故障注入测试 |

## 快速开始

```bash
mvn -q verify          # 跑通全部 21 个测试
mvn -q -pl example test  # 只跑演示场景（会打印每个场景的最终状态与事件时间线）
```

手工运行演示 main：

```bash
mvn -q -pl example -am compile
java -cp "example/target/classes:coordinator/target/classes:participant/target/classes:$(find ~/.m2 -name 'h2-2.2.224.jar')" com.example.saga.demo.SagaDemoMain
```

## 事务定义（Java DSL）

一个 Saga 由若干**有序阶段（stage）**组成；阶段可以是单个串行步骤，也可以是一个并行分组。
每个步骤必须同时提供「正向动作」和「补偿动作」：

```java
SagaDefinition saga = SagaBuilder.named("order-saga")
        .step("account.debit",                       // 串行步骤
                ctx -> account.debit("alice", 100),
                ctx -> account.credit("alice", 100))
        .parallel("fulfil", group -> group           // 并行分组
                .step("inventory.reserve",
                        ctx -> inventory.reserve("sku-42", 2),
                        ctx -> inventory.release("sku-42", 2))
                .step("points.add",
                        ctx -> points.add("alice", 10),
                        ctx -> points.deduct("alice", 10)))
        .build();

SagaResult result = new SagaEngine(dataSource).execute(saga);
```

步骤可单独配置超时与重试：`SagaStep.of(...).withTimeout(Duration.ofMillis(100)).withRetry(new RetryPolicy(3, 50, 2.0))`。

## 事务状态机

```
                 所有步骤成功
              ┌──────────────────┐
              ▼                  │
  RUNNING ──┐              COMPLETED
     │      │ 某步骤重试耗尽
     │      ▼
     │  COMPENSATING ── 全部补偿成功 ──▶ COMPENSATED
     │      │
     │      └── 补偿重试也耗尽 ──▶ FAILED（记录原因，需人工介入）
     │
     └── 进程崩溃：状态原样留在 H2 中，重启后 recover() 从断点继续
```

- 每次状态迁移（saga 状态、步骤状态、重试次数、失败原因）都写入 H2 的
  `saga_instance` / `step_instance` 表，同时追加一条 `saga_event` 时间线事件。
- `recover()` 扫描处于 `RUNNING` / `COMPENSATING` 的实例，从 `current_stage` 继续推进；
  已完成步骤靠参与方屏障去重，不会被重复执行（见 `RecoveryTest`）。
- 重试次数耗尽时，失败原因（哪个步骤、第几次尝试、异常信息）写入 `fail_reason`，
  随后进入补偿流程；补偿也耗尽才进入终态 `FAILED`。

## 补偿语义

- 正向步骤失败（含超时、注入故障）且重试耗尽后，协调器按**逆序**补偿所有已执行的步骤
  （跨阶段倒序，阶段内倒序；并行分组中成功的分支也会被补偿）。
- **结果未知的步骤也会被补偿**：如果故障发生在正向动作调用之后（如 `failAfterStep`、超时），
  步骤虽未标记成功，但其 TRY 屏障已存在，仍会被纳入补偿集合——宁可多补偿，不可漏补偿。
- **补偿必须幂等**：同一补偿请求被投递任意多次，效果只应用一次。实现上由参与方屏障的
  唯一约束保证（见下），测试见 `CompensationIdempotencyTest` 与
  `RecoveryTest#compensatingSagaIsResumedAfterRestartAndCompensationStaysIdempotent`
  （补偿执行到一半崩溃，恢复后重发补偿，效果仍只应用一次）。

## 异常防护（参与方屏障）

`ParticipantBarrier` 为每个分支（sagaId + stepName）在 `participant_barrier` 表中维护
`TRY` / `COMPENSATE` 两条屏障记录，主键唯一约束是全部防护的原子基础：

| 异常 | 场景 | 防护原理 |
|------|------|----------|
| **空回滚** | 正向请求丢失，补偿请求先到 | 插入 COMPENSATE 屏障后检查 TRY 不存在 → 直接应答成功，**不执行**补偿逻辑（`NULL_ROLLBACK`） |
| **悬挂** | 补偿先到、正向后到 | 迟到的 TRY 发现 COMPENSATE 屏障已存在 → **拒绝执行**正向动作（`SUSPENDED`），分支永远保持已回滚状态 |
| **重复投递** | 网络重试/重启重发 | 屏障主键冲突 → 跳过执行（`DUPLICATE_SKIPPED`），正向与补偿都幂等 |

对应故障注入测试：`NullRollbackFaultInjectionTest`、`SuspensionFaultInjectionTest`。

## 超时与重试

- 每个步骤（含补偿）在独立线程中执行并以 `Future.get(timeout)` 限时，超时即取消本次尝试。
- 失败后按指数退避重试（`RetryPolicy(maxAttempts, initialBackoffMillis, multiplier)`），
  次数耗尽 → 记录 `fail_reason` → 触发补偿。
- 测试见 `SagaEngineTest#slowStepTimesOutAndIsRetriedThenCompensated` 等。

## 故障注入框架

`FaultInjector` 在步骤边界提供四个钩子（步骤编号按声明顺序从 1 开始，并行分组展开计数）：
`beforeStep` / `afterStep` / `beforeCompensation` / `afterCompensation`。
内置实现 `FaultInjection`：

```java
FaultInjection.failBeforeStep(2)    // 第 2 步正向动作执行前失败
FaultInjection.failAfterStep(2)     // 第 2 步正向动作执行后失败（结果未知）
FaultInjection.crashAfterStep(1)    // 模拟进程崩溃（只触发一次），配合 recover() 做重启恢复测试
```

## 持久化与重启恢复

- 全部状态存于内嵌 H2（支持文件库，无需任何外部服务）。
- 提示：使用文件库时建议在 JDBC URL 加 `;DB_CLOSE_DELAY=-1`（或改用连接池），
  否则最后一个连接关闭时 H2 会整库落盘并重开，延迟明显。
- `RecoveryTest` 演示：进程在「第 1 步完成后」「补偿执行到一半」两个点位崩溃，
  新进程加载同一数据库文件后 `recover()` 把事务推进到终态，且不重复已完成的工作。

## 演示场景（example 模块）

`SagaDemoMain` 依次运行并打印最终状态 + 事件时间线：

1. 无故障，事务 `COMPLETED`；
2. 第 2 步（inventory.reserve）执行前注入故障 → 逆序补偿 → `COMPENSATED`；
3. 第 2 步执行后注入故障（结果未知）→ 已执行动作仍被精确补偿一次 → `COMPENSATED`；
4. 空回滚注入：补偿先到 → `NULL_ROLLBACK`；
5. 悬挂注入：补偿先于正向到达 → 迟到的正向被 `SUSPENDED` 拒绝。
