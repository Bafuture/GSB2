package com.example.saga.demo;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Abstract "account" participant: an in-memory ledger of balances. */
public class AccountService {

    private final Map<String, Integer> balances = new ConcurrentHashMap<>();

    public AccountService(String account, int initialBalance) {
        balances.put(account, initialBalance);
    }

    public void debit(String account, int amount) {
        balances.merge(account, -amount, Integer::sum);
    }

    public void credit(String account, int amount) {
        balances.merge(account, amount, Integer::sum);
    }

    public int balance(String account) {
        return balances.getOrDefault(account, 0);
    }

    @Override
    public String toString() {
        return "account" + balances;
    }
}
