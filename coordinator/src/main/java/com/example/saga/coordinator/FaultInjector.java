package com.example.saga.coordinator;

import java.util.ArrayList;
import java.util.List;

/**
 * Deterministic fault injection for end-to-end tests. Faults can be injected
 * at two points of each step: before the forward action runs and after it ran
 * (simulating an unknown outcome). A "crash" fault propagates out of the engine
 * to simulate a process kill.
 */
public class FaultInjector {

    public enum Point {
        BEFORE_STEP,
        AFTER_STEP
    }

    private static final class Rule {
        final Point point;
        final int stepSeq;
        final boolean crash;
        int remaining;

        Rule(Point point, int stepSeq, boolean crash) {
            this.point = point;
            this.stepSeq = stepSeq;
            this.crash = crash;
            this.remaining = Integer.MAX_VALUE;
        }
    }

    private final List<Rule> rules = new ArrayList<>();

    private FaultInjector() {
    }

    public static FaultInjector none() {
        return new FaultInjector();
    }

    public FaultInjector failBeforeStep(int stepSeq) {
        rules.add(new Rule(Point.BEFORE_STEP, stepSeq, false));
        return this;
    }

    public FaultInjector failAfterStep(int stepSeq) {
        rules.add(new Rule(Point.AFTER_STEP, stepSeq, false));
        return this;
    }

    public FaultInjector crashAfterStep(int stepSeq) {
        rules.add(new Rule(Point.AFTER_STEP, stepSeq, true));
        return this;
    }

    /** Limits the most recently added rule to the first {@code n} matching invocations. */
    public FaultInjector times(int n) {
        if (rules.isEmpty()) {
            throw new IllegalStateException("no rule to limit");
        }
        rules.get(rules.size() - 1).remaining = n;
        return this;
    }

    void check(Point point, int stepSeq) {
        for (Rule rule : rules) {
            if (rule.point == point && rule.stepSeq == stepSeq && rule.remaining > 0) {
                rule.remaining--;
                String where = point == Point.BEFORE_STEP ? "before" : "after";
                if (rule.crash) {
                    throw new SagaCrashException("injected crash " + where + " step " + stepSeq);
                }
                throw new SagaFaultException("injected fault " + where + " step " + stepSeq);
            }
        }
    }
}
