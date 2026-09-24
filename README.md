# Saga 分布式事务协调器

一个基于 Saga 模式的轻量级分布式事务协调器，避免两阶段提交（2PC）的全局锁与重协调开销。
Maven 多模块工程，仅依赖内嵌 H2 与 JUnit，无外部中间件。

## 模块结构

| 模块 | 说明 |
| --- | --- |
| `coordinator` | 事务 DSL、编排引擎、状态机、H2 状态持久化、重启恢复、超时/重试、故障注入 |
| `participant` | 参与方屏障（幂等、空回滚、悬挂防护）与参与方抽象 |
| `example` | 跨账户/库存/积分三个参与方的端到端演示与场景测试 |

## 快速开始

```bash
mvn -q verify                    # 跑通全部 24 个测试
mvn -pl example -am test         # 运行演示场景并打印最终状态与事件时间线
```

演示入口：`example/src/main/java/com/example/saga/example/OrderSagaDemo.java`
（也可以在 IDE 中直接运行 `OrderSagaDemo.main`，五个注入场景的最终状态与事件时间线会打印到控制台。）

## 事务定义（Java DSL）

```java
SagaDefinition saga = Saga.define("place-order")
        .step("debit-account",
                account.forward(ctx -> debit(ctx)),
                account.compensation(ctx -> refund(ctx)))
        .parallel(group -> group                       // 并行分组：组内步骤并发执行
                .step("reserve-inventory",
                        inventory.forward(ctx -> reserve(ctx)),
                        inventory.compensation(ctx -> release(ctx)))
                .step("earn-points",
                        points.forward(ctx -> earn(ctx)),
                        points.compensation(ctx -> revoke(ctx))))
        .build();

SagaResult result = engine.execute(saga);
```

- 每个步骤由「正向动作 + 补偿动作」组成，组与组之间严格串行，并行分组内并发执行；
- 每步可单独配置超时（默认 5s）与重试策略（`RetryPolicy.of(maxAttempts, initialBackoff, multiplier)`，指数退避）。

## 事务状态机

```
                 全部步骤成功
              ┌──────────────────┐
              ▼                  │
 RUNNING ────────────────► COMPLETED
    │
    │ 某步重试耗尽 / 注入故障
    ▼
 COMPENSATING ──补偿全部完成──► COMPENSATED（终态，失败原因已落库）
    │
    │ 某步补偿重试也耗尽
    ▼
 FAILED（终态，需人工介入，原因落库）
```

步骤级状态：`PENDING → RUNNING → SUCCEEDED / FAILED → COMPENSATED`。
每一次状态迁移与重试都写入 H2 的 `saga_instance` / `saga_step` / `saga_event` 三张表，
`saga_event` 即事务的完整事件时间线。

## 状态持久化与重启恢复

- 使用内嵌 H2（`jdbc:h2:file:` 文件库即可跨进程存活），不引入任何外部数据库；
- 引擎在每个步骤执行前后落库，崩溃时状态停留在最后一个已持久化的迁移点；
- 重启后调用 `SagaEngine.recover()`：扫描所有 `RUNNING` / `COMPENSATING` 的事务，
  按已持久化的步骤状态继续推进——已成功的步骤不重跑，结果未知的步骤依赖幂等重放；
- 测试：`SagaRecoveryTest` 用故障注入模拟「步骤执行后、状态落库前进程崩溃」与
  「补偿进行到一半崩溃」两种场景，验证新进程能把事务推进到终态。

## 补偿语义

- 任一步骤正向失败（含超时、重试耗尽、注入故障）→ 事务进入 `COMPENSATING`，
  从失败步骤起**逆序**补偿所有已执行过的步骤；
- 失败步骤本身也会被补偿：超时等场景下其正向动作结果未知，补偿是最安全的选择，
  若正向实际未执行，由参与方的空回滚防护兜底（见下）；
- 补偿动作必须**幂等**：同一补偿被重复投递多次，效果与执行一次一致。
  参与方屏障以 `(participant, saga_id, step_name, phase)` 主键去重，
  重复补偿直接返回成功不再执行（`ParticipantBarrierTest.compensationIsIdempotentWhenRequestedTwice`、
  `AnomalyProtectionTest.compensationStaysIdempotentWhenCoordinatorRetriesAfterUnknownOutcome`）。

## 异常防护原理（参与方屏障）

参与方在本地屏障表中记录每个 `(saga, step)` 的 `FORWARD` / `COMPENSATE` 屏障：

- **空回滚（null rollback）**：收到补偿请求但不存在 `FORWARD` 屏障
  → 不执行补偿动作，仅插入 `COMPENSATE` 屏障并返回成功；
- **悬挂（suspension）**：正向请求迟到、到达时 `COMPENSATE` 屏障已存在
  → 拒绝执行正向动作（否则该分支将永远悬置、无法被补偿）；
- **幂等**：屏障主键唯一约束保证重复请求只生效一次；
  正向动作执行失败会删除屏障，允许协调器重试。

端到端故障注入测试见 `participant/src/test/java/com/example/saga/participant/AnomalyProtectionTest.java`。

## 超时与重试

- 单步在独立线程执行，`Future.get(timeout)` 超时即取消并判定本次尝试失败；
- 失败后按指数退避重试，次数耗尽则步骤置为 `FAILED`，触发补偿，
  失败原因（含尝试次数与最后异常）写入 `saga_instance.failure_reason`；
- 补偿同样有限重试；补偿也耗尽 → 事务置为 `FAILED` 等待人工介入。

## 故障注入框架

`FaultInjector` 支持在两个位置注入确定性故障（`stepSeq` 为步骤在事务中的序号）：

```java
engine.setFaultInjector(FaultInjector.none()
        .failBeforeStep(1)      // 第 1 步正向动作执行前失败（动作未执行）
        .failAfterStep(2));     // 第 2 步正向动作执行后失败（结果未知）
```

另有 `crashAfterStep(n)` 模拟进程崩溃（状态停留在崩溃前），用于驱动重启恢复测试。

## 关键测试索引

| 需求 | 测试 |
| --- | --- |
| 串行/并行编排、补偿逆序 | `SagaEngineTest` |
| 重启恢复 | `SagaRecoveryTest` |
| 补偿幂等 | `ParticipantBarrierTest`、`AnomalyProtectionTest` |
| 空回滚 / 悬挂 | `AnomalyProtectionTest`（故障注入端到端） |
| 超时 / 退避重试 / 失败落库 | `SagaEngineTest` |
| 三参与方演示 | `OrderSagaDemoTest` |
