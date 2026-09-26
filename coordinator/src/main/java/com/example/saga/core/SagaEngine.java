package com.example.saga.core;

import com.example.saga.participant.BarrierResult;
import com.example.saga.participant.ParticipantBarrier;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The saga orchestrator.
 *
 * <p>Executes the stages of a {@link SagaDefinition} in order (parallel groups via a
 * thread pool), persists every state transition in embedded H2, wraps every step in
 * a timeout and a bounded retry with exponential backoff, compensates already
 * executed steps in reverse order on failure, and can resume interrupted sagas after
 * a process restart via {@link #recover()}.</p>
 *
 * <p>Every forward/compensation invocation is guarded by the participant-side
 * {@link ParticipantBarrier}, giving idempotency, null-rollback and suspension
 * protection.</p>
 */
public final class SagaEngine {

    private final SagaRepository repository;
    private final ParticipantBarrier barrier;
    private final FaultInjector faultInjector;
    private final ExecutorService stageExecutor;
    private final ExecutorService attemptExecutor;
    private final Map<String, SagaDefinition> registry = new ConcurrentHashMap<>();

    public SagaEngine(DataSource dataSource) {
        this(dataSource, FaultInjector.NONE);
    }

    public SagaEngine(DataSource dataSource, FaultInjector faultInjector) {
        this.repository = new SagaRepository(dataSource);
        this.barrier = new ParticipantBarrier(dataSource);
        this.faultInjector = faultInjector;
        this.stageExecutor = Executors.newCachedThreadPool(daemonThreads("saga-stage"));
        this.attemptExecutor = Executors.newCachedThreadPool(daemonThreads("saga-attempt"));
    }

    /** Registers a definition so that {@link #recover()} can resume its instances. */
    public void register(SagaDefinition definition) {
        registry.put(definition.getName(), definition);
    }

    /** Starts a new saga instance and runs it to a terminal state (or a simulated crash). */
    public SagaResult execute(SagaDefinition definition) {
        register(definition);
        String sagaId = UUID.randomUUID().toString();
        repository.insertSaga(sagaId, definition.getName());
        int stageIndex = 0;
        for (SagaStage stage : definition.getStages()) {
            for (int stepIndex = 0; stepIndex < stage.getSteps().size(); stepIndex++) {
                repository.insertStep(sagaId, stageIndex, stepIndex, stage.getSteps().get(stepIndex).getName());
            }
            stageIndex++;
        }
        event(sagaId, "SAGA_STARTED", "saga=" + definition.getName());
        runSaga(definition, sagaId, new SagaContext(sagaId));
        return result(sagaId);
    }

    /**
     * Resumes every saga instance left in RUNNING or COMPENSATING state (e.g. after a
     * process crash/restart). Definitions must be registered first via
     * {@link #register(SagaDefinition)}.
     */
    public List<SagaResult> recover() {
        List<SagaResult> recovered = new ArrayList<>();
        for (SagaRepository.SagaRow row : repository.incompleteSagas()) {
            SagaDefinition definition = registry.get(row.sagaName());
            if (definition == null) {
                event(row.sagaId(), "RECOVERY_SKIPPED", "no registered definition for " + row.sagaName());
                continue;
            }
            event(row.sagaId(), "SAGA_RECOVERED", "resuming from stage " + row.currentStage()
                    + " in state " + row.status());
            runSaga(definition, row.sagaId(), new SagaContext(row.sagaId()));
            recovered.add(result(row.sagaId()));
        }
        return recovered;
    }

    /** Current view of a saga instance, including its event timeline. */
    public SagaResult result(String sagaId) {
        SagaRepository.SagaRow row = repository.loadSaga(sagaId);
        return new SagaResult(row.sagaId(), row.sagaName(), row.status(), row.failReason(),
                repository.events(sagaId));
    }

    public void shutdown() {
        stageExecutor.shutdownNow();
        attemptExecutor.shutdownNow();
    }

    // ------------------------------------------------------------------ internals

    private void runSaga(SagaDefinition definition, String sagaId, SagaContext context) {
        SagaRepository.SagaRow row = repository.loadSaga(sagaId);
        try {
            if (row.status() == SagaStatus.COMPENSATING) {
                compensate(definition, sagaId, context);
                return;
            }
            List<SagaStage> stages = definition.getStages();
            int stepNumber = stepNumberOfStageStart(definition, row.currentStage());
            for (int s = row.currentStage(); s < stages.size(); s++) {
                repository.updateCurrentStage(sagaId, s);
                SagaStage stage = stages.get(s);
                boolean stageOk = stage.isParallel()
                        ? executeParallelStage(definition, sagaId, context, s, stage, stepNumber)
                        : executeStepWithRetry(definition, sagaId, context, s, 0,
                                stage.getSteps().get(0), stepNumber);
                if (!stageOk) {
                    compensate(definition, sagaId, context);
                    return;
                }
                stepNumber += stage.getSteps().size();
            }
            repository.updateStatus(sagaId, SagaStatus.COMPLETED);
            event(sagaId, "SAGA_COMPLETED", null);
        } catch (SagaCrashException crash) {
            event(sagaId, "SAGA_CRASH_SIMULATED", crash.getMessage());
            throw crash;
        }
    }

    private boolean executeParallelStage(SagaDefinition definition, String sagaId, SagaContext context,
                                         int stageIndex, SagaStage stage, int firstStepNumber) {
        List<SagaStep> steps = stage.getSteps();
        List<CompletableFuture<Boolean>> futures = new ArrayList<>();
        for (int i = 0; i < steps.size(); i++) {
            final int stepIndex = i;
            futures.add(CompletableFuture.supplyAsync(
                    () -> executeStepWithRetry(definition, sagaId, context, stageIndex, stepIndex,
                            steps.get(stepIndex), firstStepNumber + stepIndex),
                    stageExecutor));
        }
        boolean allOk = true;
        for (CompletableFuture<Boolean> future : futures) {
            try {
                allOk &= future.join();
            } catch (CompletionException e) {
                throw unwrapCrash(e);
            }
        }
        return allOk;
    }

    private boolean executeStepWithRetry(SagaDefinition definition, String sagaId, SagaContext context,
                                         int stageIndex, int stepIndex, SagaStep step, int stepNumber) {
        RetryPolicy retry = step.getRetryPolicy();
        String name = step.getName();
        for (int attempt = 1; attempt <= retry.maxAttempts(); attempt++) {
            repository.updateStepStatus(sagaId, stageIndex, stepIndex, StepStatus.RUNNING, attempt);
            event(sagaId, "STEP_STARTED",
                    "step=" + name + " attempt=" + attempt + "/" + retry.maxAttempts());
            try {
                faultInjector.beforeStep(stepNumber, name);
                BarrierResult[] outcome = new BarrierResult[1];
                runWithTimeout(step, () -> outcome[0] = barrier.tryExecute(sagaId, name,
                        () -> step.getAction().execute(context)));
                if (outcome[0] == BarrierResult.SUSPENDED) {
                    throw new IllegalStateException("step '" + name + "' was suspended"
                            + " (compensation arrived before the forward action)");
                }
                faultInjector.afterStep(stepNumber, name);
                repository.updateStepStatus(sagaId, stageIndex, stepIndex, StepStatus.SUCCEEDED, attempt);
                event(sagaId, "STEP_SUCCEEDED", "step=" + name
                        + (outcome[0] == BarrierResult.DUPLICATE_SKIPPED ? " (already executed, skipped)" : ""));
                return true;
            } catch (SagaCrashException crash) {
                throw crash;
            } catch (Exception failure) {
                event(sagaId, "STEP_FAILED", "step=" + name + " attempt=" + attempt
                        + " cause=" + failure.getMessage());
                if (attempt < retry.maxAttempts()) {
                    long backoff = retry.backoffBeforeAttempt(attempt + 1);
                    event(sagaId, "STEP_RETRYING", "step=" + name + " backoffMs=" + backoff);
                    sleep(backoff);
                }
            }
        }
        repository.updateStepStatus(sagaId, stageIndex, stepIndex, StepStatus.FAILED, retry.maxAttempts());
        String reason = "step '" + name + "' (no. " + stepNumber + ") exhausted "
                + retry.maxAttempts() + " attempt(s)";
        repository.recordFailReason(sagaId, reason);
        event(sagaId, "STEP_EXHAUSTED", reason);
        return false;
    }

    private void compensate(SagaDefinition definition, String sagaId, SagaContext context) {
        repository.updateStatus(sagaId, SagaStatus.COMPENSATING);
        event(sagaId, "COMPENSATION_STARTED", null);
        boolean allCompensated = true;
        for (SagaRepository.StepRow row : repository.stepsAwaitingCompensation(sagaId)) {
            SagaStep step = stepAt(definition, row.stageIndex(), row.stepIndex());
            int stepNumber = stepNumberOf(definition, row.stageIndex(), row.stepIndex());
            if (!compensateStepWithRetry(sagaId, context, step, stepNumber)) {
                allCompensated = false;
                continue;
            }
            repository.markStepCompensated(sagaId, row.stageIndex(), row.stepIndex());
        }
        if (allCompensated) {
            repository.updateStatus(sagaId, SagaStatus.COMPENSATED);
            event(sagaId, "SAGA_COMPENSATED", null);
        } else {
            String reason = "compensation exhausted retries; manual intervention required";
            repository.recordFailReason(sagaId, reason);
            repository.updateStatus(sagaId, SagaStatus.FAILED);
            event(sagaId, "SAGA_FAILED", reason);
        }
    }

    private boolean compensateStepWithRetry(String sagaId, SagaContext context,
                                            SagaStep step, int stepNumber) {
        RetryPolicy retry = step.getRetryPolicy();
        String name = step.getName();
        for (int attempt = 1; attempt <= retry.maxAttempts(); attempt++) {
            try {
                faultInjector.beforeCompensation(stepNumber, name);
                BarrierResult[] outcome = new BarrierResult[1];
                runWithTimeout(step, () -> outcome[0] = barrier.compensate(sagaId, name,
                        () -> step.getCompensation().compensate(context)));
                faultInjector.afterCompensation(stepNumber, name);
                switch (outcome[0]) {
                    case EXECUTED -> event(sagaId, "COMPENSATION_SUCCEEDED", "step=" + name);
                    case DUPLICATE_SKIPPED -> event(sagaId, "COMPENSATION_SUCCEEDED",
                            "step=" + name + " (already compensated, skipped)");
                    case NULL_ROLLBACK -> event(sagaId, "NULL_ROLLBACK",
                            "step=" + name + " (forward action never ran)");
                    default -> throw new IllegalStateException("unexpected barrier result " + outcome[0]);
                }
                return true;
            } catch (SagaCrashException crash) {
                throw crash;
            } catch (Exception failure) {
                event(sagaId, "COMPENSATION_FAILED", "step=" + name + " attempt=" + attempt
                        + " cause=" + failure.getMessage());
                if (attempt < retry.maxAttempts()) {
                    sleep(retry.backoffBeforeAttempt(attempt + 1));
                }
            }
        }
        event(sagaId, "COMPENSATION_EXHAUSTED", "step=" + name);
        return false;
    }

    private void runWithTimeout(SagaStep step, ThrowingRunnable runnable) throws Exception {
        Future<?> future = attemptExecutor.submit(() -> {
            try {
                runnable.run();
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        });
        try {
            future.get(step.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new StepTimeoutException(
                    "timed out after " + step.getTimeout().toMillis() + "ms");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof CompletionException ce && ce.getCause() != null) {
                cause = ce.getCause();
            }
            if (cause instanceof SagaCrashException crash) {
                throw crash;
            }
            if (cause instanceof Exception exception) {
                throw exception;
            }
            throw new RuntimeException(cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("interrupted while waiting for step", e);
        }
    }

    private static SagaCrashException unwrapCrash(CompletionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof SagaCrashException crash) {
            return crash;
        }
        return new SagaCrashException("parallel stage aborted: " + cause);
    }

    private SagaStep stepAt(SagaDefinition definition, int stageIndex, int stepIndex) {
        return definition.getStages().get(stageIndex).getSteps().get(stepIndex);
    }

    private int stepNumberOf(SagaDefinition definition, int stageIndex, int stepIndex) {
        return stepNumberOfStageStart(definition, stageIndex) + stepIndex;
    }

    private int stepNumberOfStageStart(SagaDefinition definition, int stageIndex) {
        int number = 1;
        for (int s = 0; s < stageIndex; s++) {
            number += definition.getStages().get(s).getSteps().size();
        }
        return number;
    }

    private void event(String sagaId, String type, String detail) {
        repository.addEvent(sagaId, type, detail);
    }

    private static ThreadFactory daemonThreads(String prefix) {
        AtomicInteger counter = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, prefix + "-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    /** Raised when a step does not finish within its configured timeout. */
    public static class StepTimeoutException extends RuntimeException {
        public StepTimeoutException(String message) {
            super(message);
        }
    }
}
