package com.example.saga.demo;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Abstract "loyalty points" participant: an in-memory points ledger. */
public class PointsService {

    private final Map<String, Integer> points = new ConcurrentHashMap<>();

    public void add(String member, int amount) {
        points.merge(member, amount, Integer::sum);
    }

    public void deduct(String member, int amount) {
        points.merge(member, -amount, Integer::sum);
    }

    public int points(String member) {
        return points.getOrDefault(member, 0);
    }

    @Override
    public String toString() {
        return "points" + points;
    }
}
