package com.example.saga.core;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Configurable {@link FaultInjector} supporting the two required injection points
 * ("fail before step N" / "fail after step N") plus simulated crashes used by the
 * recovery tests. Crash faults fire only once, so a restarted engine can proceed.
 */
public final class FaultInjection implements FaultInjector {

    private Integer failBeforeStep;
    private Integer failAfterStep;
    private Integer crashBeforeStep;
    private Integer crashAfterStep;
    private Integer crashAfterCompensationOfStep;
    private final Set<String> firedCrashes = ConcurrentHashMap.newKeySet();

    public static FaultInjection none() {
        return new FaultInjection();
    }

    /** Injects a failure right before the forward action of step {@code n} (fires on every attempt). */
    public static FaultInjection failBeforeStep(int n) {
        FaultInjection f = new FaultInjection();
        f.failBeforeStep = n;
        return f;
    }

    /** Injects a failure right after the forward action of step {@code n} (fires on every attempt). */
    public static FaultInjection failAfterStep(int n) {
        FaultInjection f = new FaultInjection();
        f.failAfterStep = n;
        return f;
    }

    /** Simulates a process crash right before step {@code n} (fires once). */
    public static FaultInjection crashBeforeStep(int n) {
        FaultInjection f = new FaultInjection();
        f.crashBeforeStep = n;
        return f;
    }

    /** Simulates a process crash right after the forward action of step {@code n} (fires once). */
    public static FaultInjection crashAfterStep(int n) {
        FaultInjection f = new FaultInjection();
        f.crashAfterStep = n;
        return f;
    }

    /** Additionally simulates a crash right after the compensation of step {@code n} (fires once). */
    public FaultInjection andCrashAfterCompensationOfStep(int n) {
        this.crashAfterCompensationOfStep = n;
        return this;
    }

    @Override
    public void beforeStep(int stepNumber, String stepName) {
        crashOnce(crashBeforeStep, stepNumber, "cb", "simulated crash before step " + stepNumber);
        if (failBeforeStep != null && failBeforeStep == stepNumber) {
            throw new FaultInjectedException(
                    "injected fault before step " + stepNumber + " (" + stepName + ")");
        }
    }

    @Override
    public void afterStep(int stepNumber, String stepName) {
        crashOnce(crashAfterStep, stepNumber, "ca", "simulated crash after step " + stepNumber);
        if (failAfterStep != null && failAfterStep == stepNumber) {
            throw new FaultInjectedException(
                    "injected fault after step " + stepNumber + " (" + stepName + ")");
        }
    }

    @Override
    public void afterCompensation(int stepNumber, String stepName) {
        crashOnce(crashAfterCompensationOfStep, stepNumber, "cc",
                "simulated crash after compensation of step " + stepNumber);
    }

    private void crashOnce(Integer configured, int stepNumber, String kind, String message) {
        if (configured != null && configured == stepNumber && firedCrashes.add(kind + stepNumber)) {
            throw new SagaCrashException(message);
        }
    }
}
