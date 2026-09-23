package io.saga.core.engine;

import io.saga.core.EventType;
import io.saga.core.RetryPolicy;
import io.saga.core.SagaContext;
import io.saga.core.SagaDefinition;
import io.saga.core.SagaStatus;
import io.saga.core.StepDefinition;
import io.saga.core.StepStatus;
import io.saga.core.fault.FaultInjector;
import io.saga.core.fault.InjectedFaultException;
import io.saga.core.store.SagaStore;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Saga orchestrator. Executes the forward plan, persists every transition,
 * compensates in reverse order on failure, and resumes interrupted sagas
 * from the store after a restart.
 *
 * <p>Anomaly guards:
 * <ul>
 *   <li>empty rollback - compensation is only delivered for steps whose forward
 *       action completed (status SUCCEEDED);</li>
 *   <li>dangling - a forward execution that arrives after its step was already
 *       compensated is skipped;</li>
 *   <li>idempotent compensation - a DB compare-and-set (SUCCEEDED -&gt; COMPENSATING)
 *       guarantees the compensation action is delivered at most once per step.</li>
 * </ul>
 */
public class SagaOrchestrator implements AutoCloseable {

    private final SagaStore store;
    private final ExecutorService executor;
    private final FaultInjector faultInjector;
    private final Duration defaultStepTimeout;
    private final RetryPolicy defaultRetryPolicy;
    private final Map<String, SagaDefinition> definitions = new ConcurrentHashMap<>();

    private SagaOrchestrator(Builder builder) {
        this.store = builder.store;
        this.faultInjector = builder.faultInjector;
        this.defaultStepTimeout = builder.defaultStepTimeout;
        this.defaultRetryPolicy = builder.defaultRetryPolicy;
        this.executor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "saga-worker");
            t.setDaemon(true);
            return t;
        });
    }

    public static Builder builder(SagaStore store) {
        return new Builder(store);
    }

    /** Register a definition so {@link #recover()} can resume its interrupted instances. */
    public void register(SagaDefinition definition) {
        definitions.put(definition.getName(), definition);
    }

    public SagaResult execute(SagaDefinition definition) {
        return execute(definition, new SagaContext(UUID.randomUUID().toString()));
    }

    public SagaResult execute(SagaDefinition definition, SagaContext context) {
        register(definition);
        String sagaId = context.getSagaId();
        store.createSaga(sagaId, definition.getName());
        for (StepDefinition step : definition.getFlatSteps()) {
            store.insertStep(sagaId, step.getIndex(), step.getName(), step.getGroupId());
        }
        store.appendEvent(sagaId, EventType.SAGA_STARTED, "saga=" + definition.getName());
        runForward(sagaId, definition, context);
        return result(sagaId);
    }

    /** Resume one persisted saga (used by recovery and by the dangling-anomaly demo). */
    public SagaResult resume(String sagaId, SagaDefinition definition, SagaContext context) {
        register(definition);
        SagaStore.SagaRecord record = store.loadSaga(sagaId)
                .orElseThrow(() -> new IllegalArgumentException("unknown saga " + sagaId));
        if (record.status() == SagaStatus.RUNNING) {
            runForward(sagaId, definition, context);
        } else if (record.status() == SagaStatus.COMPENSATING) {
            compensate(sagaId, definition, context, record.failureReason());
        }
        return result(sagaId);
    }

    /**
     * Resume every saga the store still marks RUNNING or COMPENSATING.
     * Returns the number of sagas that were picked up.
     */
    public int recover() {
        int resumed = 0;
        for (SagaStore.SagaRecord record : store.loadIncomplete()) {
            SagaDefinition definition = definitions.get(record.sagaName());
            if (definition == null) {
                continue;
            }
            store.appendEvent(record.sagaId(), EventType.SAGA_RECOVERED, "resumed from status " + record.status());
            SagaContext context = new SagaContext(record.sagaId());
            if (record.status() == SagaStatus.RUNNING) {
                runForward(record.sagaId(), definition, context);
            } else {
                compensate(record.sagaId(), definition, context, record.failureReason());
            }
            resumed++;
        }
        return resumed;
    }

    /**
     * Explicit rollback entry point. Safe to call repeatedly: already compensated
     * steps are skipped, proving idempotent compensation at the engine level.
     */
    public SagaResult rollback(String sagaId) {
        SagaStore.SagaRecord record = store.loadSaga(sagaId)
                .orElseThrow(() -> new IllegalArgumentException("unknown saga " + sagaId));
        if (record.status() == SagaStatus.COMPLETED || record.status() == SagaStatus.FAILED) {
            return result(sagaId);
        }
        SagaDefinition definition = definitions.get(record.sagaName());
        if (definition == null) {
            throw new IllegalStateException("saga definition not registered: " + record.sagaName());
        }
        compensate(sagaId, definition, new SagaContext(sagaId), "manual rollback requested");
        return result(sagaId);
    }

    public SagaResult result(String sagaId) {
        SagaStore.SagaRecord record = store.loadSaga(sagaId)
                .orElseThrow(() -> new IllegalArgumentException("unknown saga " + sagaId));
        return new SagaResult(sagaId, record.status(), record.failureReason(), store.loadEvents(sagaId));
    }

    private void runForward(String sagaId, SagaDefinition definition, SagaContext context) {
        try {
            for (SagaDefinition.PlanItem item : definition.getPlan()) {
                if (item instanceof SagaDefinition.StepItem single) {
                    runStep(sagaId, single.step(), context);
                } else if (item instanceof SagaDefinition.GroupItem group) {
                    runGroup(sagaId, group, context);
                }
            }
            store.updateSagaStatus(sagaId, SagaStatus.COMPLETED, null);
            store.appendEvent(sagaId, EventType.SAGA_COMPLETED, null);
        } catch (StepFailureException failure) {
            compensate(sagaId, definition, context, failure.getMessage());
        }
    }

    private void runGroup(String sagaId, SagaDefinition.GroupItem group, SagaContext context) {
        List<CompletableFuture<?>> futures = new ArrayList<>();
        for (StepDefinition step : group.steps()) {
            futures.add(CompletableFuture.runAsync(() -> runStep(sagaId, step, context), executor));
        }
        StepFailureException firstFailure = null;
        for (CompletableFuture<?> future : futures) {
            try {
                future.join();
            } catch (java.util.concurrent.CompletionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof Error error) {
                    throw error; // simulated crash must escape untouched
                }
                if (firstFailure == null && cause instanceof StepFailureException sfe) {
                    firstFailure = sfe;
                } else if (firstFailure == null) {
                    firstFailure = new StepFailureException(String.valueOf(cause));
                }
            }
        }
        if (firstFailure != null) {
            throw firstFailure;
        }
    }

    private void runStep(String sagaId, StepDefinition step, SagaContext context) {
        int index = step.getIndex();
        StepStatus status = currentStatus(sagaId, index);
        switch (status) {
            case SUCCEEDED -> {
                return; // already done (recovery re-entrant safety)
            }
            case COMPENSATED, COMPENSATION_FAILED -> {
                // dangling protection: the compensation arrived before the forward action
                store.appendEvent(sagaId, EventType.FORWARD_SKIPPED_DANGLING, step.getName());
                return;
            }
            case FAILED -> throw new StepFailureException(
                    "step '" + step.getName() + "' already failed; saga must compensate");
            case PENDING -> {
                if (!store.casStepStatus(sagaId, index, StepStatus.PENDING, StepStatus.RUNNING)) {
                    return; // someone else owns this step
                }
            }
            case RUNNING -> store.appendEvent(sagaId, EventType.STEP_RESUMED,
                    step.getName() + " (re-run after crash)");
            default -> throw new IllegalStateException("unexpected step status " + status);
        }

        FaultInjector.Decision decision;
        try {
            decision = faultInjector.beforeStep(new FaultInjector.StepPoint(sagaId, index, step.getName()));
        } catch (InjectedFaultException e) {
            store.forceStepStatus(sagaId, index, StepStatus.FAILED);
            store.appendEvent(sagaId, EventType.STEP_FAILED, step.getName() + "  " + e.getMessage());
            throw new StepFailureException(e.getMessage());
        } catch (Exception e) {
            store.forceStepStatus(sagaId, index, StepStatus.FAILED);
            throw new StepFailureException("fault injector failed before step '" + step.getName() + "': " + e);
        }
        if (decision == FaultInjector.Decision.SKIP_ACTION) {
            // forward delivery lost: step looks successful although the action never ran
            store.forceStepStatus(sagaId, index, StepStatus.SUCCEEDED);
            store.appendEvent(sagaId, EventType.STEP_FORWARD_DROPPED, step.getName());
            return;
        }

        RetryPolicy retry = step.getRetryPolicy() != null ? step.getRetryPolicy() : defaultRetryPolicy;
        Duration timeout = step.getTimeout() != null ? step.getTimeout() : defaultStepTimeout;
        int maxAttempts = retry.getMaxAttempts();
        Throwable lastError = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            store.setStepAttempts(sagaId, index, attempt);
            store.appendEvent(sagaId,
                    attempt == 1 ? EventType.STEP_STARTED : EventType.STEP_RETRY,
                    step.getName() + " attempt=" + attempt);
            try {
                invoke(step.getForward(), step.getName(), context, timeout);
                faultInjector.afterStep(new FaultInjector.StepPoint(sagaId, index, step.getName()));
                store.casStepStatus(sagaId, index, StepStatus.RUNNING, StepStatus.SUCCEEDED);
                store.appendEvent(sagaId, EventType.STEP_SUCCEEDED, step.getName());
                return;
            } catch (InjectedFaultException e) {
                // failure AFTER the action ran: the effect is uncertain, so the step
                // is recorded SUCCEEDED and will be included in compensation
                store.forceStepStatus(sagaId, index, StepStatus.SUCCEEDED);
                store.appendEvent(sagaId, EventType.STEP_FAILED, step.getName() + "  " + e.getMessage());
                throw new StepFailureException(e.getMessage());
            } catch (TimeoutException e) {
                lastError = e;
                store.appendEvent(sagaId, EventType.STEP_TIMEOUT,
                        step.getName() + " attempt=" + attempt + " timed out after " + timeout.toMillis() + "ms");
            } catch (Exception e) {
                lastError = e;
                store.appendEvent(sagaId, EventType.STEP_FAILED,
                        step.getName() + " attempt=" + attempt + " error=" + e.getMessage());
            }
            if (attempt < maxAttempts) {
                sleep(retry.backoffForFailure(attempt));
            }
        }
        store.forceStepStatus(sagaId, index, StepStatus.FAILED);
        throw new StepFailureException("step '" + step.getName() + "' failed after " + maxAttempts
                + " attempt(s): " + lastError);
    }

    private void compensate(String sagaId, SagaDefinition definition, SagaContext context, String reason) {
        store.updateSagaStatus(sagaId, SagaStatus.COMPENSATING, reason);
        store.appendEvent(sagaId, EventType.SAGA_COMPENSATING, reason);
        List<SagaStore.StepRecord> steps = store.loadSteps(sagaId);
        boolean allCompensated = true;
        for (int i = steps.size() - 1; i >= 0; i--) { // strictly reverse order
            SagaStore.StepRecord record = steps.get(i);
            StepDefinition step = definition.getStep(record.index());
            switch (record.status()) {
                case SUCCEEDED, COMPENSATING -> {
                    if (record.status() == StepStatus.SUCCEEDED
                            && !store.casStepStatus(sagaId, record.index(),
                                    StepStatus.SUCCEEDED, StepStatus.COMPENSATING)) {
                        // lost the race: another compensator already took this step
                        store.appendEvent(sagaId, EventType.COMPENSATION_SKIPPED_DUPLICATE, step.getName());
                        continue;
                    }
                    if (!compensateStep(sagaId, step, context)) {
                        allCompensated = false;
                    }
                }
                case COMPENSATED -> store.appendEvent(sagaId,
                        EventType.COMPENSATION_SKIPPED_DUPLICATE, step.getName());
                default -> store.appendEvent(sagaId,
                        EventType.COMPENSATION_SKIPPED_EMPTY_ROLLBACK,
                        step.getName() + " (forward never completed)");
            }
        }
        if (allCompensated) {
            store.updateSagaStatus(sagaId, SagaStatus.COMPENSATED, reason);
            store.appendEvent(sagaId, EventType.SAGA_COMPENSATED, reason);
        } else {
            String failedReason = reason + " | compensation incomplete, manual intervention required";
            store.updateSagaStatus(sagaId, SagaStatus.FAILED, failedReason);
            store.appendEvent(sagaId, EventType.SAGA_FAILED, failedReason);
        }
    }

    private boolean compensateStep(String sagaId, StepDefinition step, SagaContext context) {
        int index = step.getIndex();
        RetryPolicy retry = step.getRetryPolicy() != null ? step.getRetryPolicy() : defaultRetryPolicy;
        Duration timeout = step.getTimeout() != null ? step.getTimeout() : defaultStepTimeout;
        int maxAttempts = retry.getMaxAttempts();
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            store.appendEvent(sagaId,
                    attempt == 1 ? EventType.COMPENSATION_STARTED : EventType.COMPENSATION_RETRY,
                    step.getName() + " attempt=" + attempt);
            try {
                invoke(step.getCompensation(), step.getName(), context, timeout);
                store.casStepStatus(sagaId, index, StepStatus.COMPENSATING, StepStatus.COMPENSATED);
                store.appendEvent(sagaId, EventType.COMPENSATION_SUCCEEDED, step.getName());
                return true;
            } catch (Exception e) {
                store.appendEvent(sagaId, EventType.COMPENSATION_FAILED,
                        step.getName() + " attempt=" + attempt + " error=" + e.getMessage());
                if (attempt < maxAttempts) {
                    sleep(retry.backoffForFailure(attempt));
                }
            }
        }
        store.forceStepStatus(sagaId, index, StepStatus.COMPENSATION_FAILED);
        return false;
    }

    private void invoke(io.saga.core.SagaAction action, String stepName,
                        SagaContext context, Duration timeout) throws Exception {
        Future<Object> future = executor.submit(() -> {
            SagaContext.setCurrentStep(stepName);
            try {
                action.execute(context);
            } finally {
                SagaContext.clearCurrentStep();
            }
            return null;
        });
        try {
            future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw e;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Error error) {
                throw error;
            }
            throw (Exception) cause;
        }
    }

    private StepStatus currentStatus(String sagaId, int index) {
        for (SagaStore.StepRecord record : store.loadSteps(sagaId)) {
            if (record.index() == index) {
                return record.status();
            }
        }
        throw new IllegalStateException("no persisted step " + index + " for saga " + sagaId);
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }

    public static final class Builder {
        private final SagaStore store;
        private FaultInjector faultInjector = FaultInjector.NONE;
        private Duration defaultStepTimeout = Duration.ofSeconds(5);
        private RetryPolicy defaultRetryPolicy = RetryPolicy.of(3, Duration.ofMillis(100), 2.0);

        private Builder(SagaStore store) {
            this.store = store;
        }

        public Builder faultInjector(FaultInjector faultInjector) {
            this.faultInjector = faultInjector;
            return this;
        }

        public Builder defaultStepTimeout(Duration timeout) {
            this.defaultStepTimeout = timeout;
            return this;
        }

        public Builder defaultRetryPolicy(RetryPolicy retryPolicy) {
            this.defaultRetryPolicy = retryPolicy;
            return this;
        }

        public SagaOrchestrator build() {
            return new SagaOrchestrator(this);
        }
    }
}
