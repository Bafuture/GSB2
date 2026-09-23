package io.saga.participant;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Base class for saga participants. Implements the three classic TCC/Saga
 * anomaly guards around a pair of (forward, compensation) effects:
 *
 * <ul>
 *   <li><b>Idempotency</b> - a compensation delivered twice applies its effect once;
 *       a duplicate forward delivery is ignored.</li>
 *   <li><b>Empty rollback</b> - a compensation whose forward never arrived is a
 *       successful no-op, and the key is remembered...</li>
 *   <li><b>Dangling</b> - ...so that a forward arriving AFTER its compensation
 *       is rejected instead of leaving uncommitted effects behind.</li>
 * </ul>
 *
 * Keys are {@code sagaId:stepName} provided by the engine via the context.
 */
public abstract class Participant {

    private final String name;
    private final Set<String> appliedKeys = ConcurrentHashMap.newKeySet();
    private final Set<String> compensatedKeys = ConcurrentHashMap.newKeySet();
    private final List<String> opLog = new CopyOnWriteArrayList<>();

    protected Participant(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    /** Guarded forward effect. */
    protected final synchronized void forward(String key, String operation, Runnable effect) {
        if (compensatedKeys.contains(key)) {
            log("DANGLING_FORWARD_REJECTED " + operation);
            return;
        }
        if (!appliedKeys.add(key)) {
            log("DUPLICATE_FORWARD_IGNORED " + operation);
            return;
        }
        effect.run();
        log("FORWARD " + operation);
    }

    /** Guarded compensation effect. */
    protected final synchronized void compensate(String key, String operation, Runnable effect) {
        if (!appliedKeys.contains(key)) {
            // empty rollback: nothing to undo; record the key so a late forward is rejected
            compensatedKeys.add(key);
            log("EMPTY_ROLLBACK_IGNORED " + operation);
            return;
        }
        if (compensatedKeys.contains(key)) {
            log("DUPLICATE_COMPENSATION_IGNORED " + operation);
            return;
        }
        effect.run();
        compensatedKeys.add(key);
        log("COMPENSATED " + operation);
    }

    private void log(String entry) {
        opLog.add(name + ": " + entry);
    }

    /** Ordered, human-readable operation log (used by tests and the demo). */
    public List<String> getOperationLog() {
        return List.copyOf(opLog);
    }
}
