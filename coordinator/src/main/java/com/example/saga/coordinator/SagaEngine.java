package com.example.saga.coordinator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Orchestrates saga execution: runs step groups in order (steps inside a parallel
 * group run concurrently), persists every transition, compensates on failure in
 * reverse order, and resumes unfinished sagas after a restart via {@link #recover()}.
 */
public class SagaEngine implements AutoCloseable {

    private final SagaRepository repository;
    private final ExecutorService executor;
    private final Map<String, SagaDefinition> definitions = new ConcurrentHashMap<>();
    private volatile FaultInjector faultInjector = FaultInjector.none();

    public SagaEngine(SagaRepository repository) {
        this.repository = repository;
        this.executor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "saga-engine");
            t.setDaemon(true);
            return t;
        });
    }

    /** Registers a definition so that {@link #recover()} can resume its unfinished instances. */
    public void register(SagaDefinition definition) {
        definitions.put(definition.name(), definition);
    }

    public void setFaultInjector(FaultInjector faultInjector) {
        this.faultInjector = faultInjector;
    }

    public SagaResult execute(SagaDefinition definition) {
        return execute(definition, UUID.randomUUID().toString());
    }

    public SagaResult execute(SagaDefinition definition, String sagaId) {
        register(definition);
        repository.createSaga(sagaId, definition.name());
        List<SagaStep> steps = definition.flattenedSteps();
        for (int seq = 0; seq < steps.size(); seq++) {
            repository.createStep(sagaId, seq, steps.get(seq).name());
        }
        repository.recordEvent(sagaId, "SAGA_STARTED", null, "saga=" + definition.name());
        driveForward(sagaId, definition, new SagaContext(sagaId, definition.name()));
        return result(sagaId);
    }

    /**
     * Resumes every saga left in RUNNING or COMPENSATING state (e.g. after a process
     * restart) from its persisted step states. Returns the number of resumed sagas.
     */
    public int recover() {
        int recovered = 0;
        for (SagaRepository.SagaRecord record : repository.findIncompleteSagas()) {
            SagaDefinition definition = definitions.get(record.sagaName());
            if (definition == null) {
                throw new IllegalStateException(
                        "no saga definition registered for '" + record.sagaName() + "', cannot recover " + record.sagaId());
            }
            repository.recordEvent(record.sagaId(), "SAGA_RECOVERY", null,
                    "resuming from status " + record.status());
            SagaContext ctx = new SagaContext(record.sagaId(), record.sagaName());
            if (record.status() == SagaStatus.COMPENSATING) {
                runCompensations(record.sagaId(), definition, ctx,
                        record.failureReason() == null ? "recovered" : record.failureReason());
            } else {
                driveForward(record.sagaId(), definition, ctx);
            }
            recovered++;
        }
        return recovered;
    }

    public SagaResult result(String sagaId) {
        SagaRepository.SagaRecord record = repository.findSaga(sagaId)
                .orElseThrow(() -> new IllegalArgumentException("unknown saga " + sagaId));
        return new SagaResult(sagaId, record.status(), record.failureReason(), repository.loadEvents(sagaId));
    }

    public List<SagaEvent> timeline(String sagaId) {
        return repository.loadEvents(sagaId);
    }

    private void driveForward(String sagaId, SagaDefinition definition, SagaContext ctx) {
        List<SagaStep> steps = definition.flattenedSteps();
        try {
            int seq = 0;
            for (StepGroup group : definition.groups()) {
                runGroup(sagaId, group, steps, seq, ctx);
                seq += group.steps().size();
            }
            repository.updateSagaStatus(sagaId, SagaStatus.COMPLETED, null);
            repository.recordEvent(sagaId, "SAGA_COMPLETED", null, null);
        } catch (StepFailure failure) {
            repository.updateSagaStatus(sagaId, SagaStatus.COMPENSATING, failure.getMessage());
            repository.recordEvent(sagaId, "SAGA_COMPENSATING", null, failure.getMessage());
            runCompensations(sagaId, definition, ctx, failure.getMessage());
        }
    }

    private void runGroup(String sagaId, StepGroup group, List<SagaStep> allSteps, int firstSeq, SagaContext ctx) {
        Map<Integer, SagaRepository.StepRecord> persisted = repository.loadSteps(sagaId).stream()
                .collect(Collectors.toMap(SagaRepository.StepRecord::seq, Function.identity()));
        List<Integer> pendingSeqs = new ArrayList<>();
        for (int i = 0; i < group.steps().size(); i++) {
            int seq = firstSeq + i;
            if (persisted.get(seq).status() != StepStatus.SUCCEEDED) {
                pendingSeqs.add(seq);
            }
        }
        if (pendingSeqs.isEmpty()) {
            return;
        }
        if (!group.parallel() || pendingSeqs.size() == 1) {
            for (int seq : pendingSeqs) {
                runForward(sagaId, seq, allSteps.get(seq), ctx, persisted.get(seq).attempts());
            }
            return;
        }
        List<Future<?>> futures = new ArrayList<>();
        for (int seq : pendingSeqs) {
            SagaStep step = allSteps.get(seq);
            int priorAttempts = persisted.get(seq).attempts();
            futures.add(executor.submit(() -> runForward(sagaId, seq, step, ctx, priorAttempts)));
        }
        StepFailure firstFailure = null;
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (ExecutionException e) {
                if (e.getCause() instanceof SagaCrashException crash) {
                    throw crash;
                }
                if (firstFailure == null && e.getCause() instanceof StepFailure failure) {
                    firstFailure = failure;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while running parallel group", e);
            }
        }
        if (firstFailure != null) {
            throw firstFailure;
        }
    }

    private void runForward(String sagaId, int seq, SagaStep step, SagaContext ctx, int priorAttempts) {
        repository.updateStepStatus(sagaId, seq, StepStatus.RUNNING, priorAttempts);
        repository.recordEvent(sagaId, "STEP_STARTED", step.name(), null);
        int attempts = priorAttempts;
        while (true) {
            attempts++;
            try {
                faultInjector.check(FaultInjector.Point.BEFORE_STEP, seq);
                runWithTimeout(step.forward(), ctx, step);
                faultInjector.check(FaultInjector.Point.AFTER_STEP, seq);
                repository.updateStepStatus(sagaId, seq, StepStatus.SUCCEEDED, attempts);
                repository.recordEvent(sagaId, "STEP_SUCCEEDED", step.name(), "attempts=" + attempts);
                return;
            } catch (SagaCrashException crash) {
                throw crash;
            } catch (SagaFaultException fault) {
                repository.updateStepStatus(sagaId, seq, StepStatus.FAILED, attempts);
                repository.recordEvent(sagaId, "STEP_FAILED", step.name(), fault.getMessage());
                throw new StepFailure("step '" + step.name() + "' failed: " + fault.getMessage());
            } catch (Exception e) {
                if (attempts >= step.retryPolicy().maxAttempts()) {
                    repository.updateStepStatus(sagaId, seq, StepStatus.FAILED, attempts);
                    repository.recordEvent(sagaId, "STEP_FAILED", step.name(),
                            "exhausted " + attempts + " attempt(s): " + e.getMessage());
                    throw new StepFailure("step '" + step.name() + "' failed after "
                            + attempts + " attempt(s): " + e.getMessage());
                }
                repository.updateStepStatus(sagaId, seq, StepStatus.RUNNING, attempts);
                repository.recordEvent(sagaId, "STEP_RETRY", step.name(),
                        "attempt " + attempts + " failed: " + e.getMessage());
                sleep(step.retryPolicy().backoffMillisForAttempt(attempts));
            }
        }
    }

    private void runCompensations(String sagaId, SagaDefinition definition, SagaContext ctx, String reason) {
        List<SagaStep> steps = definition.flattenedSteps();
        Map<Integer, SagaRepository.StepRecord> persisted = repository.loadSteps(sagaId).stream()
                .collect(Collectors.toMap(SagaRepository.StepRecord::seq, Function.identity()));
        for (int seq = steps.size() - 1; seq >= 0; seq--) {
            StepStatus status = persisted.get(seq).status();
            if (status == StepStatus.PENDING || status == StepStatus.COMPENSATED) {
                continue;
            }
            SagaStep step = steps.get(seq);
            try {
                runCompensation(sagaId, seq, step, ctx);
            } catch (SagaCrashException crash) {
                throw crash;
            } catch (Exception e) {
                String message = reason + "; compensation of step '" + step.name()
                        + "' exhausted retries: " + e.getMessage();
                repository.updateSagaStatus(sagaId, SagaStatus.FAILED, message);
                repository.recordEvent(sagaId, "SAGA_FAILED", step.name(), message);
                return;
            }
        }
        repository.updateSagaStatus(sagaId, SagaStatus.COMPENSATED, reason);
        repository.recordEvent(sagaId, "SAGA_COMPENSATED", null, reason);
    }

    private void runCompensation(String sagaId, int seq, SagaStep step, SagaContext ctx) {
        repository.recordEvent(sagaId, "COMPENSATION_STARTED", step.name(), null);
        int attempts = 0;
        while (true) {
            attempts++;
            try {
                runWithTimeout(step.compensation(), ctx, step);
                repository.updateStepStatus(sagaId, seq, StepStatus.COMPENSATED, attempts);
                repository.recordEvent(sagaId, "STEP_COMPENSATED", step.name(), "attempts=" + attempts);
                return;
            } catch (SagaCrashException crash) {
                throw crash;
            } catch (Exception e) {
                if (attempts >= step.retryPolicy().maxAttempts()) {
                    repository.updateStepStatus(sagaId, seq, StepStatus.FAILED, attempts);
                    throw new IllegalStateException(e.getMessage(), e);
                }
                repository.recordEvent(sagaId, "COMPENSATION_RETRY", step.name(),
                        "attempt " + attempts + " failed: " + e.getMessage());
                sleep(step.retryPolicy().backoffMillisForAttempt(attempts));
            }
        }
    }

    private void runWithTimeout(SagaAction action, SagaContext ctx, SagaStep step) throws Exception {
        Future<?> future = executor.submit(() -> {
            ctx.enterStep(step.name());
            try {
                action.execute(ctx);
            } finally {
                ctx.leaveStep();
            }
            return null;
        });
        try {
            future.get(step.timeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new StepTimeoutException(
                    "timed out after " + step.timeout().toMillis() + "ms");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception exception) {
                throw exception;
            }
            throw new RuntimeException(cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while executing step", e);
        }
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
        executor.shutdownNow();
    }

    private static final class StepFailure extends RuntimeException {
        StepFailure(String message) {
            super(message);
        }
    }

    /** Thrown when a single step execution exceeds its configured timeout. */
    public static class StepTimeoutException extends RuntimeException {
        public StepTimeoutException(String message) {
            super(message);
        }
    }
}
