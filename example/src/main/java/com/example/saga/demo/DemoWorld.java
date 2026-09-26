package com.example.saga.demo;

import com.example.saga.core.SagaBuilder;
import com.example.saga.core.SagaDefinition;

/**
 * A self-contained demo world: the three participants plus the saga definition
 * that spans them.
 *
 * <pre>
 *   step 1: account.debit          (serial)
 *   step 2: inventory.reserve  ┐
 *   step 3: points.add         ┘  parallel group "fulfil"
 * </pre>
 */
public class DemoWorld {

    public static final String ACCOUNT = "alice";
    public static final String SKU = "sku-42";
    public static final String MEMBER = "alice";

    public final AccountService account = new AccountService(ACCOUNT, 1_000);
    public final InventoryService inventory = new InventoryService();
    public final PointsService points = new PointsService();

    public SagaDefinition orderSaga() {
        return SagaBuilder.named("order-saga")
                .step("account.debit",
                        ctx -> account.debit(ACCOUNT, 100),
                        ctx -> account.credit(ACCOUNT, 100))
                .parallel("fulfil", group -> group
                        .step("inventory.reserve",
                                ctx -> inventory.reserve(SKU, 2),
                                ctx -> inventory.release(SKU, 2))
                        .step("points.add",
                                ctx -> points.add(MEMBER, 10),
                                ctx -> points.deduct(MEMBER, 10)))
                .build();
    }

    public String participantStates() {
        return account + "  " + inventory + "  " + points;
    }
}
