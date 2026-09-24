package com.example.saga.participant;

import com.example.saga.coordinator.SagaAction;
import com.example.saga.coordinator.SagaContext;

import javax.sql.DataSource;
import java.util.function.Consumer;

/**
 * A saga participant. Wraps business actions with a {@link ParticipantBarrier}
 * so that forward and compensation requests are idempotent and protected
 * against null rollbacks and suspension.
 */
public class Participant {

    private final String name;
    private final ParticipantBarrier barrier;

    public Participant(String name, DataSource dataSource) {
        this.name = name;
        this.barrier = new ParticipantBarrier(dataSource, name);
    }

    public String name() {
        return name;
    }

    public ParticipantBarrier barrier() {
        return barrier;
    }

    public void setLogger(Consumer<String> logger) {
        barrier.setLogger(logger);
    }

    /** Wraps a forward action with barrier protection; saga/step identity comes from the context. */
    public SagaAction forward(SagaAction action) {
        return ctx -> barrier.tryForward(ctx.sagaId(), stepName(ctx), action, ctx);
    }

    /** Wraps a compensation action with barrier protection; saga/step identity comes from the context. */
    public SagaAction compensation(SagaAction action) {
        return ctx -> barrier.tryCompensate(ctx.sagaId(), stepName(ctx), action, ctx);
    }

    private static String stepName(SagaContext ctx) {
        String stepName = ctx.stepName();
        if (stepName == null) {
            throw new IllegalStateException("no current step in saga context");
        }
        return stepName;
    }
}
