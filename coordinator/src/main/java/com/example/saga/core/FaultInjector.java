package com.example.saga.core;

/**
 * Hook points for fault injection. Step numbers are 1-based and count all steps of
 * the saga in declaration order (parallel groups expanded).
 */
public interface FaultInjector {

    FaultInjector NONE = new FaultInjector() {
    };

    /** Called immediately before the forward action of step {@code stepNumber} runs. */
    default void beforeStep(int stepNumber, String stepName) {
    }

    /** Called immediately after the forward action of step {@code stepNumber} succeeded. */
    default void afterStep(int stepNumber, String stepName) {
    }

    /** Called immediately before the compensation of step {@code stepNumber} runs. */
    default void beforeCompensation(int stepNumber, String stepName) {
    }

    /** Called immediately after the compensation of step {@code stepNumber} succeeded. */
    default void afterCompensation(int stepNumber, String stepName) {
    }
}
