package com.example.saga.core;

import java.util.ArrayList;
import java.util.List;

/** An immutable saga definition: an ordered list of serial/parallel stages. */
public final class SagaDefinition {

    private final String name;
    private final List<SagaStage> stages;

    SagaDefinition(String name, List<SagaStage> stages) {
        this.name = name;
        this.stages = List.copyOf(stages);
    }

    public String getName() {
        return name;
    }

    public List<SagaStage> getStages() {
        return stages;
    }

    /** All steps in execution order (parallel groups expanded in declaration order). */
    public List<SagaStep> flattenSteps() {
        List<SagaStep> all = new ArrayList<>();
        for (SagaStage stage : stages) {
            all.addAll(stage.getSteps());
        }
        return all;
    }
}
