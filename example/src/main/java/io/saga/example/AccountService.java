package io.saga.example;

import io.saga.participant.Participant;
import java.util.concurrent.atomic.AtomicInteger;

/** Demo participant: a ledger account. All actions go through the barrier. */
public final class AccountService {

    private final Participant participant;
    private final AtomicInteger balance = new AtomicInteger(1_000);

    public AccountService(Participant participant) {
        this.participant = participant;
    }

    public void debit(String txId, String stepId, int amount) throws Exception {
        boolean executed = participant.forward(txId, stepId, () -> {
            balance.addAndGet(-amount);
            System.out.printf("    [account]   debit  %4d -> balance=%d%n", amount, balance.get());
        });
        if (!executed) {
            System.out.printf("    [account]   debit  %4d suppressed by barrier (balance=%d)%n", amount, balance.get());
        }
    }

    public void refund(String txId, String stepId, int amount) throws Exception {
        boolean executed = participant.compensate(txId, stepId, () -> {
            balance.addAndGet(amount);
            System.out.printf("    [account]   refund %4d -> balance=%d%n", amount, balance.get());
        });
        if (!executed) {
            System.out.printf("    [account]   refund %4d suppressed by barrier (balance=%d)%n", amount, balance.get());
        }
    }

    public int balance() {
        return balance.get();
    }
}
