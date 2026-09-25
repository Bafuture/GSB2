package io.saga.example;

import io.saga.participant.Participant;
import java.util.concurrent.atomic.AtomicInteger;

/** Demo participant: stock reservation. */
public final class InventoryService {

    private final Participant participant;
    private final AtomicInteger stock = new AtomicInteger(50);

    public InventoryService(Participant participant) {
        this.participant = participant;
    }

    public void reserve(String txId, String stepId, int quantity) throws Exception {
        boolean executed = participant.forward(txId, stepId, () -> {
            stock.addAndGet(-quantity);
            System.out.printf("    [inventory] reserve %4d -> stock=%d%n", quantity, stock.get());
        });
        if (!executed) {
            System.out.printf("    [inventory] reserve %4d suppressed by barrier (stock=%d)%n", quantity, stock.get());
        }
    }

    public void release(String txId, String stepId, int quantity) throws Exception {
        boolean executed = participant.compensate(txId, stepId, () -> {
            stock.addAndGet(quantity);
            System.out.printf("    [inventory] release %4d -> stock=%d%n", quantity, stock.get());
        });
        if (!executed) {
            System.out.printf("    [inventory] release %4d suppressed by barrier (stock=%d)%n", quantity, stock.get());
        }
    }

    public int stock() {
        return stock.get();
    }
}
