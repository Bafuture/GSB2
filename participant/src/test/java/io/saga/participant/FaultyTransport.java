package io.saga.participant;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Test double simulating an unreliable network between coordinator and
 * participant: it can drop, delay and reorder forward/compensation requests.
 */
final class FaultyTransport {

    private record Key(String txId, String stepId) {
    }

    private record Pending(String txId, String stepId, ParticipantAction action) {
    }

    private final Barrier barrier;
    private final Set<Key> droppedForwards = new HashSet<>();
    private final Set<Key> delayedForwards = new HashSet<>();
    private final Map<Key, ParticipantAction> dropped = new HashMap<>();
    private final List<Pending> delayed = new ArrayList<>();

    FaultyTransport(Barrier barrier) {
        this.barrier = barrier;
    }

    void dropForward(String txId, String stepId) {
        droppedForwards.add(new Key(txId, stepId));
    }

    void delayForward(String txId, String stepId) {
        delayedForwards.add(new Key(txId, stepId));
    }

    boolean forward(String txId, String stepId, ParticipantAction action) throws Exception {
        Key key = new Key(txId, stepId);
        if (droppedForwards.remove(key)) {
            dropped.put(key, action);
            return false; // swallowed by the "network"
        }
        if (delayedForwards.remove(key)) {
            delayed.add(new Pending(txId, stepId, action));
            return false; // parked, delivered later
        }
        return barrier.forward(txId, stepId, action);
    }

    boolean compensate(String txId, String stepId, ParticipantAction action) throws Exception {
        return barrier.compensate(txId, stepId, action);
    }

    /** Delivers previously dropped forwards; returns true if any action actually ran. */
    boolean deliverDropped() throws Exception {
        boolean ran = false;
        for (Map.Entry<Key, ParticipantAction> entry : dropped.entrySet()) {
            ran |= barrier.forward(entry.getKey().txId(), entry.getKey().stepId(), entry.getValue());
        }
        dropped.clear();
        return ran;
    }

    /** Delivers previously delayed forwards; returns true if any action actually ran. */
    boolean deliverDelayed() throws Exception {
        boolean ran = false;
        for (Pending pending : delayed) {
            ran |= barrier.forward(pending.txId(), pending.stepId(), pending.action());
        }
        delayed.clear();
        return ran;
    }
}
