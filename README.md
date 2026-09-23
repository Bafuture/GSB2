# Saga 分布式事务协调器

一个基于 Saga 模式的轻量级分布式事务协调器（Maven 多模块，Java 17，内嵌 H2 持久化，无外部依赖服务）。

## 模块结构

| 模块 | 说明 |
|------|------|
| `coordinator` | 核心：事务定义 DSL、状态机、H2 持久化、崩溃恢复、超时重试、故障注入框架 |
| `participant` | 参与方抽象（账户 / 库存 / 积分），内置幂等、空回滚、悬挂三重防护 |
| `example` | 端到端演示：跨三个参与方的订单事务，跑遍所有故障注入场景并打印事件时间线 |

## 快速开始

```bash
mvn -q verify   # 编译 + 全部测试 + 运行 example 演示
```

### 用 DSL 定义事务

```java
SagaDefinition saga = Saga.define("order-saga")
        .step("debit-account", account.debit(80_00), account.refund(80_00))
        .parallel("prepare", p -> p                              // 至多一个并行分组
                .step("reserve-stock", inventory.reserve(3), inventory.release(3))
                .step("add-points",   points.add(120),   points.revoke(120)))
        .step(StepDefinition.of("confirm", confirm, cancel)
                .timeout(Duration.ofMillis(500))                 // 单步超时
                .retry(RetryPolicy.of(3, Duration.ofMillis(50), 2.0)) // 退避重试
                .build())
        .build();

SagaOrchestrator orchestrator = SagaOrchestrator.builder(JdbcSagaStore.file(dir, "saga")).build();
orchestrator.register(saga);            // 注册后才能被 recover() 恢复
SagaResult result = orchestrator.execute(saga);
```

每一步由「正向动作 + 补偿动作」组成；串行步骤按声明顺序执行，并行分组内的步骤并发执行、整组同成败。

## 事务状态机

### Saga 实例状态

```
                 所有正向步骤成功
              ┌─────────────────────┐
              ▼                     │
  RUNNING ──正向步骤失败──▶ COMPENSATING ──补偿全部完成──▶ COMPENSATED
    ▲                         │
    │                         └──补偿重试耗尽──▶ FAILED（需人工介入）
    └── recover() 只会拾取 RUNNING / COMPENSATING 两种非终态
```

- `COMPLETED`：全部正向步骤成功（终态）。
- `COMPENSATED`：正向失败后补偿全部完成，系统回到一致状态（终态），失败原因已落库。
- `FAILED`：补偿也失败了，状态机无法自动收敛，必须人工处理（终态），原因已落库。

### 步骤状态

```
PENDING ──▶ RUNNING ──▶ SUCCEEDED ──▶ COMPENSATING ──▶ COMPENSATED
              │              ▲                            ▲
              └──重试耗尽──▶ FAILED                       │
                           （触发整体补偿）                └── 空回滚：正向未执行，
                                                            直接置为 COMPENSATED
```

关键转换使用数据库 compare-and-set（如 `UPDATE ... WHERE status='SUCCEEDED'`），
这是幂等补偿与并发防护的根基。

## 状态持久化与崩溃恢复

- 三张表全部落在内嵌 H2（`JdbcSagaStore`，支持内存与文件两种模式，不引入外部数据库）：
  - `saga_instance`：事务状态与失败原因；
  - `saga_step`：每一步的状态与已尝试次数；
  - `saga_event`：完整事件时间线（演示与排障用）。
- 进程崩溃（测试用 `SimulatedCrashException` 模拟 kill -9）后，事务停留在 `RUNNING`/`COMPENSATING`。
- 重启后调用 `orchestrator.recover()`：扫描非终态事务，按持久化的步骤状态续跑——
  已 `SUCCEEDED` 的步骤跳过，未完成的步骤继续推进；`COMPENSATING` 中的事务继续未完成的补偿。
- 对应测试：`SagaOrchestratorTest#restartRecoversAndCompletesInterruptedSaga`、
  `restartResumesInterruptedCompensation`（文件型 H2 + 全新 orchestrator 实例模拟重启）。

## 补偿语义

1. **逆序**：正向步骤失败时，按严格逆序补偿所有已成功的步骤（含并行分组内已成功的步骤）。
2. **幂等**：
   - 引擎层——补偿前执行 CAS `SUCCEEDED → COMPENSATING`，抢不到就跳过，同一补偿在同一进程内最多投递一次；
   - 参与方层——以 `sagaId:stepName` 为幂等键记录已补偿集合，重复投递只生效一次。
   - 测试：`compensationIsIdempotentAcrossRepeatedRollbackRequests`、`compensationAppliedMultipleTimesHasSameResultAsOnce`。
3. **失败即失败**：补偿本身也按退避策略重试，耗尽后步骤置 `COMPENSATION_FAILED`，事务置 `FAILED` 并记录原因，等待人工介入。

## 异常防护（空回滚与悬挂）

两道防线，引擎层 + 参与方层：

| 异常 | 定义 | 引擎层防护 | 参与方层防护 |
|------|------|-----------|-------------|
| **空回滚** | 正向动作未执行就收到补偿请求 | 只补偿 `SUCCEEDED` 的步骤；`PENDING`/`FAILED` 步骤跳过并记录 `COMPENSATION_SKIPPED_EMPTY_ROLLBACK` | 补偿键不在已执行集合中 → 成功 no-op，并把键记入已补偿集合 |
| **悬挂** | 补偿先到、正向动作后到 | 正向执行前检查步骤状态，已是 `COMPENSATED` 则跳过并记录 `FORWARD_SKIPPED_DANGLING` | 空回滚时已记录补偿键，迟到的正向动作发现键已补偿 → 拒绝执行 |

故障注入测试：`FaultInjectionScenariosTest#emptyRollbackScenario`（用 `dropForwardOfStep` 丢弃正向投递）、
`#danglingScenario`（预置已补偿状态模拟补偿先到）、`ParticipantGuardTest`（参与方层单元证明）。

## 超时与重试

- 每个正向/补偿动作在线程池中执行并受 `Future.get(timeout)` 约束，超时即取消（可每步覆盖，默认 5s）。
- 失败后按 `RetryPolicy(最大次数, 初始退避, 倍率)` 指数退避重试；尝试次数持久化在 `saga_step.attempts`。
- 重试耗尽：正向步骤 → 触发整体补偿，事务终态 `COMPENSATED` 并记录原因；补偿步骤 → 事务终态 `FAILED` 并记录原因。
- 测试：`stepTimeoutRetriesWithBackoffThenRollsBack`、`exhaustedCompensationRetriesMarkSagaFailedWithReason`。

## 故障注入框架

`ConfigurableFaultInjector` 支持在步骤执行前后注入确定性故障（注入故障不可重试，立即失败）：

```java
new ConfigurableFaultInjector()
        .failBeforeStep(2)      // 第 2 步执行前失败
        .failAfterStep(1)       // 第 1 步执行后失败（效果不确定 → 该步也参与补偿）
        .dropForwardOfStep(1)   // 丢弃第 1 步的正向投递（驱动空回滚）
        .crashBeforeStep(2);    // 模拟进程崩溃（驱动恢复测试，仅触发一次）
```

## Example 演示

`mvn -q verify` 的最后会运行 `example` 模块，依次演示六个场景并打印最终状态与持久化的事件时间线：

1. 无故障 happy path；
2. 第 N 步执行前注入失败；
3. 第 N 步执行后注入失败；
4. 空回滚（正向投递丢失后收到补偿）；
5. 悬挂（补偿先于正向到达）；
6. 崩溃后从 H2 文件恢复并续跑完成。
