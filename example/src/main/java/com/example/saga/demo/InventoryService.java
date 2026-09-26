package com.example.saga.demo;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Abstract "inventory" participant: tracks reserved stock per SKU. */
public class InventoryService {

    private final Map<String, Integer> reserved = new ConcurrentHashMap<>();

    public void reserve(String sku, int quantity) {
        reserved.merge(sku, quantity, Integer::sum);
    }

    public void release(String sku, int quantity) {
        reserved.merge(sku, -quantity, Integer::sum);
    }

    public int reserved(String sku) {
        return reserved.getOrDefault(sku, 0);
    }

    @Override
    public String toString() {
        return "inventory" + reserved;
    }
}
