# GSB2 — Saga 分布式事务协调器

一个基于 Saga 模式的轻量级分布式事务协调器，替代两阶段提交（2PC）。
Maven 多模块工程：

| 模块 | 职责 |
| --- | --- |
| `coordinator` | 事务 DSL、状态机、H2 持久化、超时/重试、崩溃恢复、故障注入 |
| `participant` | 参与方屏障（幂等、空回滚、悬挂防护） |
| `example` | 跨账户/库存/积分三个参与方的端到端演示 |

## 快速开始

```bash
mvn -q verify                      # 跑通全部测试
mvn -q install -DskipTests && mvn -q -pl example exec:java   # 运行演示
```

## 事务定义（Java DSL）

```java
SagaDefinition saga = Saga.define("order-saga")
        .retry(RetryPolicy.exponential(3, Duration.ofMillis(50), 2.0))
        .stepTimeout(Duration.ofSeconds(1))
        .step("debit",   ctx -> account.debit(...),    ctx -> account.refund(...))
        .step("reserve", ctx -> inventory.reserve(...), ctx -> inventory.release(...))
        .parallel("extras", p -> p                      // 可选：一个并行分组
                .step("points", ctx -> points.add(...),    ctx -> points.deduct(...))
                .step("coupon", ctx -> coupon.issue(...),  ctx -> coupon.void(...)))
        .build();

String sagaId = orchestrator.submit(saga);   // 异步执行，状态全程落库
```

每个步骤由「正向动作 + 补偿动作」组成；步骤默认串行，`parallel(...)` 分组内的
步骤并发执行，全部分支成功才继续推进。

## 事务状态机

```
                 全部步骤成功
        RUNNING ──────────────► COMPLETED
           │
           │ 某步重试耗尽 / 注入故障
           ▼
      COMPENSATING ──补偿全部成功──► COMPENSATED   （失败终态，fail_reason 已记录）
           │
           │ 补偿本身重试耗尽
           ▼
         FAILED                     （失败终态，需人工介入，fail_reason 已记录）
```

步骤状态机：`PENDING → RUNNING → SUCCEEDED / FAILED → COMPENSATED`。

- `COMPENSATED` 与 `FAILED` 都是失败终态；原始失败原因始终持久化在
  `saga_instance.fail_reason` 与 `saga_event` 时间线中。
- 每一次状态迁移都同步写入内嵌 H2（`saga_instance` / `saga_step` / `saga_event`
  三张表），不依赖任何外部数据库。

## 重启恢复

进程崩溃时（测试中用 `CrashException` 模拟），已持久化的状态原样保留。
重启后新建 `SagaOrchestrator`、重新 `register(...)` 事务定义并调用
`recover()`：

- `RUNNING` 的事务从**第一个未成功的步骤**继续推进，已 `SUCCEEDED` 的步骤
  不会重复执行（`RecoveryTest` 用文件型 H2 验证，断言正向动作只执行一次）；
- `COMPENSATING` 的事务继续完成剩余的补偿。

## 补偿语义

- 正向链任意一步失败（重试耗尽、超时、注入故障）→ 事务进入 `COMPENSATING`，
  对所有 `SUCCEEDED` 步骤按**逆序**执行补偿动作。
- 补偿同样受超时与退避重试保护；补偿重试也耗尽 → 事务标记 `FAILED`。
- **幂等性**由参与方屏障保证：补偿请求被重复投递时，屏障表中
  `(tx_id, step_id)` 的 `COMPENSATED` 记录使重复请求成为空操作，
  补偿动作只执行一次（`BarrierIdempotencyTest` 证明）。

## 异常防护（参与方屏障）

`participant` 模块的 `Barrier` 以屏障表（主键 `tx_id + step_id + branch`）
记录每个分支的执行状态，防护两类经典异常：

- **空回滚（null rollback）**：正向请求在网络中丢失，补偿请求却到达了。
  屏障发现该分支没有任何记录 → 不执行补偿动作，直接插入一条
  `COMPENSATED` 屏障并返回成功。见 `NullRollbackFaultInjectionTest`。
- **悬挂（hanging）**：消息乱序导致补偿先到、正向后到。空回滚已写入
  `COMPENSATED` 屏障，迟到的正向请求被屏障拒绝，补偿结果不会被"撤销"。
  见 `HangingFaultInjectionTest`。

正向请求的重复投递同理被 `FORWARD_DONE` 屏障去重。

## 超时与重试

- 每一步（含补偿）在独立线程中执行并受 `stepTimeout` 约束，超时即取消并记为失败。
- 失败后按 `RetryPolicy`（指数退避，可配置次数/初始间隔/倍率）有限重试。
- 重试次数耗尽 → 步骤标记 `FAILED`，事务进入补偿流程，失败原因
  （含尝试次数与最后异常）持久化到 `fail_reason` 与事件时间线。

## 故障注入框架

`FaultInjector` 支持在两个位置对任意步骤注入确定性故障，用于驱动端到端测试：

```java
new FaultInjector()
    .failBefore("reserve")        // 第 N 步执行前失败
    .failAfter("reserve")         // 第 N 步执行并持久化后失败
    .crashAfter("debit", latch);  // 模拟进程崩溃（配合恢复测试）
```

`example` 模块的 `OrderSagaDemo` 依次演示：正常流程、执行前故障、执行后故障、
空回滚、悬挂、重试耗尽六个场景，并打印每个场景的最终状态与持久化事件时间线。

## 测试一览（`mvn -q verify`，共 14 个用例）

- `SagaOrchestratorTest`：正常完成、逆序补偿、并行分组、退避重试成功、
  重试耗尽记录失败原因、单步超时。
- `FaultInjectionTest`：第 N 步执行前/执行后注入故障的端到端补偿验证。
- `RecoveryTest`：崩溃后从 H2 文件库恢复并继续推进，已完成步骤不重复执行。
- `BarrierIdempotencyTest` / `NullRollbackFaultInjectionTest` /
  `HangingFaultInjectionTest`：幂等、空回滚、悬挂。
- `OrderSagaDemoTest`：六个演示场景的最终状态断言。
