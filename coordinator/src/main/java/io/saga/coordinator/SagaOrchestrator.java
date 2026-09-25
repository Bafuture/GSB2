package io.saga.coordinator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Saga orchestrator. Executes the forward chain, persists every transition,
 * retries failed steps with backoff, compensates in reverse order on failure,
 * and resumes interrupted sagas from the persisted state after a restart.
 */
public final class SagaOrchestrator implements AutoCloseable {

    private final SagaRepository repository;
    private final FaultInjector faultInjector;
    private final Map<String, SagaDefinition> definitions = new ConcurrentHashMap<>();
    private final ExecutorService stepExecutor = Executors.newCachedThreadPool();
    private final ExecutorService sagaExecutor = Executors.newCachedThreadPool();

    public SagaOrchestrator(SagaRepository repository) {
        this(repository, FaultInjector.none());
    }

    public SagaOrchestrator(SagaRepository repository, FaultInjector faultInjector) {
        this.repository = repository;
        this.faultInjector = faultInjector;
    }

    /** Registers a definition so incomplete sagas of this type can be recovered after a restart. */
    public SagaOrchestrator register(SagaDefinition definition) {
        definitions.put(definition.name(), definition);
        return this;
    }

    /** Starts a new saga instance asynchronously and returns its id. */
    public String submit(SagaDefinition definition) {
        return submit(definition, new SagaContext(newSagaId()));
    }

    public String submit(SagaDefinition definition, SagaContext ctx) {
        register(definition);
        String sagaId = ctx.sagaId();
        repository.insertSaga(sagaId, definition.name());
        int seq = 0;
        for (Phase phase : definition.phases()) {
            if (phase instanceof SagaStep step) {
                repository.insertStep(sagaId, step.name(), seq++, null);
            } else {
                ParallelGroup group = (ParallelGroup) phase;
                for (SagaStep step : group.steps()) {
                    repository.insertStep(sagaId, step.name(), seq++, group.name());
                }
            }
        }
        sagaExecutor.submit(() -> runSaga(sagaId, definition, ctx, false));
        return sagaId;
    }

    /**
     * Resumes every saga that was left in RUNNING or COMPENSATING state
     * (e.g. by a process crash). Returns the number of sagas resubmitted.
     */
    public int recover() {
        int resumed = 0;
        for (SagaInstance saga : repository.findIncomplete()) {
            SagaDefinition definition = definitions.get(saga.name());
            if (definition == null) {
                repository.updateSagaStatus(saga.sagaId(), SagaStatus.FAILED,
                        "no definition registered for saga '" + saga.name() + "'; cannot recover");
                repository.addEvent(saga.sagaId(), "SAGA_FAILED", "recovery impossible: definition not registered");
                continue;
            }
            SagaContext ctx = new SagaContext(saga.sagaId());
            if (saga.status() == SagaStatus.COMPENSATING) {
                sagaExecutor.submit(() -> compensate(saga.sagaId(), definition, ctx,
                        saga.failReason() == null ? "recovered while compensating" : saga.failReason()));
            } else {
                sagaExecutor.submit(() -> runSaga(saga.sagaId(), definition, ctx, true));
            }
            resumed++;
        }
        return resumed;
    }

    public SagaInstance saga(String sagaId) {
        return repository.findSaga(sagaId)
                .orElseThrow(() -> new IllegalArgumentException("unknown saga " + sagaId));
    }

    public List<StepRecord> steps(String sagaId) {
        return repository.loadSteps(sagaId);
    }

    public List<SagaEvent> timeline(String sagaId) {
        return repository.loadEvents(sagaId);
    }

    /** Blocks until the saga reaches a terminal state or the timeout elapses. */
    public SagaInstance awaitTerminal(String sagaId, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            SagaInstance saga = saga(sagaId);
            if (saga.status().isTerminal()) {
                return saga;
            }
            sleep(20);
        }
        throw new IllegalStateException("saga " + sagaId + " did not reach a terminal state within " + timeout);
    }

    private void runSaga(String sagaId, SagaDefinition definition, SagaContext ctx, boolean resume) {
        try {
            repository.addEvent(sagaId, resume ? "SAGA_RESUMED" : "SAGA_STARTED",
                    resume ? "recovered from persisted state" : "saga '" + definition.name() + "' started");
            for (Phase phase : definition.phases()) {
                if (phase instanceof SagaStep step) {
                    runStep(sagaId, definition, ctx, step);
                } else {
                    runGroup(sagaId, definition, ctx, (ParallelGroup) phase);
                }
            }
            repository.updateSagaStatus(sagaId, SagaStatus.COMPLETED, null);
            repository.addEvent(sagaId, "SAGA_COMPLETED", "all steps succeeded");
        } catch (CrashException crash) {
            // Simulated process crash: stop immediately without touching persisted state.
            return;
        } catch (StepFailedException failure) {
            compensate(sagaId, definition, ctx, failure.getMessage());
        } catch (InjectedFaultException fault) {
            compensate(sagaId, definition, ctx, fault.getMessage());
        }
    }

    private void runStep(String sagaId, SagaDefinition definition, SagaContext ctx, SagaStep step)
            throws StepFailedException {
        StepRecord persisted = repository.findStep(sagaId, step.name()).orElseThrow();
        if (persisted.status() == StepStatus.SUCCEEDED) {
            return; // already done before a crash; do not re-execute
        }
        RetryPolicy retry = definition.retryPolicy();
        int attempts = persisted.attempts();
        while (true) {
            faultInjector.check(FaultInjector.Position.BEFORE_STEP, step.name());
            attempts++;
            repository.updateStep(sagaId, step.name(), StepStatus.RUNNING, attempts, null);
            repository.addEvent(sagaId, "STEP_STARTED", step.name() + " (attempt " + attempts + ")");
            try {
                runWithTimeout(step.action(), ctx, definition.stepTimeout());
                repository.updateStep(sagaId, step.name(), StepStatus.SUCCEEDED, attempts, null);
                repository.addEvent(sagaId, "STEP_SUCCEEDED", step.name());
                faultInjector.check(FaultInjector.Position.AFTER_STEP, step.name());
                return;
            } catch (CrashException | InjectedFaultException e) {
                throw e;
            } catch (Exception e) {
                String reason = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
                repository.updateStep(sagaId, step.name(), StepStatus.RUNNING, attempts, reason);
                if (attempts >= retry.maxAttempts()) {
                    repository.updateStep(sagaId, step.name(), StepStatus.FAILED, attempts, reason);
                    repository.addEvent(sagaId, "STEP_FAILED",
                            step.name() + ": attempts exhausted (" + attempts + "): " + reason);
                    throw new StepFailedException(step.name(),
                            "attempts exhausted (" + attempts + "): " + reason);
                }
                repository.addEvent(sagaId, "STEP_RETRY",
                        step.name() + " attempt " + attempts + " failed: " + reason + "; retrying");
                sleep(retry.backoffFor(attempts).toMillis());
            }
        }
    }

    private void runGroup(String sagaId, SagaDefinition definition, SagaContext ctx, ParallelGroup group)
            throws StepFailedException {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (SagaStep step : group.steps()) {
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    runStep(sagaId, definition, ctx, step);
                } catch (StepFailedException | InjectedFaultException | CrashException e) {
                    throw new CompletionException(e);
                }
            }, stepExecutor));
        }
        try {
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof CompletionException ce && ce.getCause() != null) {
                cause = ce.getCause();
            }
            if (cause instanceof CrashException crash) {
                throw crash;
            }
            if (cause instanceof InjectedFaultException injected) {
                throw injected;
            }
            if (cause instanceof StepFailedException failed) {
                throw failed;
            }
            throw new StepFailedException(group.name(), String.valueOf(cause));
        }
    }

    private void compensate(String sagaId, SagaDefinition definition, SagaContext ctx, String reason) {
        repository.updateSagaStatus(sagaId, SagaStatus.COMPENSATING, reason);
        repository.addEvent(sagaId, "SAGA_COMPENSATING", reason);
        List<StepRecord> succeeded = repository.loadSteps(sagaId).stream()
                .filter(s -> s.status() == StepStatus.SUCCEEDED)
                .sorted(Comparator.comparingInt(StepRecord::seq).reversed())
                .toList();
        for (StepRecord record : succeeded) {
            SagaStep step = definition.findStep(record.stepName()).orElse(null);
            if (step == null) {
                continue;
            }
            if (!compensateStep(sagaId, definition, ctx, step, record.attempts())) {
                repository.updateSagaStatus(sagaId, SagaStatus.FAILED,
                        "compensation of step '" + step.name() + "' exhausted retries; original failure: " + reason);
                repository.addEvent(sagaId, "SAGA_FAILED",
                        "compensation of step '" + step.name() + "' failed permanently; manual intervention required");
                return;
            }
        }
        repository.updateSagaStatus(sagaId, SagaStatus.COMPENSATED, reason);
        repository.addEvent(sagaId, "SAGA_COMPENSATED", "all succeeded steps compensated in reverse order");
    }

    private boolean compensateStep(String sagaId, SagaDefinition definition, SagaContext ctx,
                                   SagaStep step, int forwardAttempts) {
        RetryPolicy retry = definition.retryPolicy();
        for (int attempt = 1; attempt <= retry.maxAttempts(); attempt++) {
            repository.addEvent(sagaId, "COMPENSATION_STARTED", step.name() + " (attempt " + attempt + ")");
            try {
                runWithTimeout(step.compensation(), ctx, definition.stepTimeout());
                repository.updateStep(sagaId, step.name(), StepStatus.COMPENSATED, forwardAttempts, null);
                repository.addEvent(sagaId, "COMPENSATION_SUCCEEDED", step.name());
                return true;
            } catch (Exception e) {
                String reason = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
                repository.addEvent(sagaId, "COMPENSATION_RETRY",
                        step.name() + " attempt " + attempt + " failed: " + reason);
                if (attempt < retry.maxAttempts()) {
                    sleep(retry.backoffFor(attempt).toMillis());
                }
            }
        }
        return false;
    }

    private void runWithTimeout(SagaAction action, SagaContext ctx, Duration timeout) throws Exception {
        Future<?> future = stepExecutor.submit(() -> {
            action.run(ctx);
            return null;
        });
        try {
            future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new Exception("step timed out after " + timeout.toMillis() + " ms", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception exception) {
                throw exception;
            }
            throw new Exception(String.valueOf(cause), cause);
        }
    }

    private static String newSagaId() {
        return UUID.randomUUID().toString();
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        sagaExecutor.shutdownNow();
        stepExecutor.shutdownNow();
    }
}
