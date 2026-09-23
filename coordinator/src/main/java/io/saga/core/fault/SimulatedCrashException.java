package io.saga.core.fault;

/**
 * Simulates a process crash (kill -9): an Error, so the engine does NOT catch
 * it, does NOT compensate, and leaves the saga RUNNING in the store for a
 * later recovery pass.
 */
public class SimulatedCrashException extends Error {
    public SimulatedCrashException(String message) {
        super(message);
    }
}
