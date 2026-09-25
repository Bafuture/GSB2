package io.saga.coordinator;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Deterministic fault-injection hook used by end-to-end tests.
 * Faults can be placed at two positions around any step:
 * <ul>
 *   <li>{@link Position#BEFORE_STEP} - before the forward action runs</li>
 *   <li>{@link Position#AFTER_STEP} - after the forward action ran and its
 *       success was persisted (models "response lost after commit")</li>
 * </ul>
 */
public final class FaultInjector {

    public enum Position {
        BEFORE_STEP,
        AFTER_STEP
    }

    private record Rule(Position position, String stepName, AtomicInteger remaining,
                        Supplier<RuntimeException> error, Runnable onTrigger) {
    }

    private final List<Rule> rules = new CopyOnWriteArrayList<>();

    public static FaultInjector none() {
        return new FaultInjector();
    }

    /** Fail once before the given step's action executes. */
    public FaultInjector failBefore(String stepName) {
        return failBefore(stepName, 1);
    }

    public FaultInjector failBefore(String stepName, int times) {
        return failBefore(stepName, times, () -> new InjectedFaultException(
                "injected fault before step '" + stepName + "'"));
    }

    public FaultInjector failBefore(String stepName, int times, Supplier<RuntimeException> error) {
        rules.add(new Rule(Position.BEFORE_STEP, stepName, new AtomicInteger(times), error, null));
        return this;
    }

    /** Fail once after the given step's action has executed and been persisted. */
    public FaultInjector failAfter(String stepName) {
        return failAfter(stepName, 1);
    }

    public FaultInjector failAfter(String stepName, int times) {
        rules.add(new Rule(Position.AFTER_STEP, stepName, new AtomicInteger(times),
                () -> new InjectedFaultException("injected fault after step '" + stepName + "'"), null));
        return this;
    }

    /**
     * Simulate a process crash after the given step completed. The returned
     * latch-free variant leaves the saga in RUNNING state for recovery tests.
     */
    public FaultInjector crashAfter(String stepName, Runnable onTrigger) {
        rules.add(new Rule(Position.AFTER_STEP, stepName, new AtomicInteger(1),
                () -> new CrashException("simulated crash after step '" + stepName + "'"), onTrigger));
        return this;
    }

    void check(Position position, String stepName) {
        for (Rule rule : rules) {
            if (rule.position() == position && rule.stepName().equals(stepName)
                    && rule.remaining().getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
                if (rule.onTrigger() != null) {
                    rule.onTrigger().run();
                }
                throw rule.error().get();
            }
        }
    }
}
