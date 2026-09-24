package com.example.saga.coordinator;

import java.util.ArrayList;
import java.util.List;

public class SagaDefinition {
    private final String name;
    private final List<StepGroup> groups;

    SagaDefinition(String name, List<StepGroup> groups) {
        this.name = name;
        this.groups = List.copyOf(groups);
    }

    public String name() {
        return name;
    }

    public List<StepGroup> groups() {
        return groups;
    }

    /** Flattens all steps in execution order; the list index is the durable step sequence number. */
    public List<SagaStep> flattenedSteps() {
        List<SagaStep> all = new ArrayList<>();
        for (StepGroup group : groups) {
            all.addAll(group.steps());
        }
        return all;
    }
}
