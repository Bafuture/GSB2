package io.saga.core.dsl;

import io.saga.core.SagaAction;
import io.saga.core.SagaDefinition;
import io.saga.core.StepDefinition;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Fluent DSL for a saga definition:
 * <pre>
 * Saga.define("order")
 *     .step("debit", debitAction, refundAction)
 *     .parallel("prepare", p -&gt; p
 *         .step("stock", reserve, release)
 *         .step("points", add, revoke))
 *     .step("confirm", confirm, cancel)
 *     .build();
 * </pre>
 * At most one parallel group is allowed per saga.
 */
public final class SagaBuilder {

    private final String name;
    private final List<SagaDefinition.PlanItem> plan = new ArrayList<>();
    private final Set<String> stepNames = new HashSet<>();
    private boolean parallelGroupUsed = false;

    public SagaBuilder(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("saga name must not be blank");
        }
        this.name = name;
    }

    public SagaBuilder step(String stepName, SagaAction forward, SagaAction compensation) {
        return step(StepDefinition.of(stepName, forward, compensation).build());
    }

    public SagaBuilder step(StepDefinition step) {
        if (step.getGroupId() != null) {
            throw new IllegalArgumentException("grouped steps must be added via parallel(...)");
        }
        checkUnique(step.getName());
        plan.add(new SagaDefinition.StepItem(step));
        return this;
    }

    public SagaBuilder parallel(String groupId, Consumer<ParallelGroupBuilder> groupSpec) {
        if (parallelGroupUsed) {
            throw new IllegalStateException("saga '" + name + "' already has a parallel group");
        }
        parallelGroupUsed = true;
        ParallelGroupBuilder group = new ParallelGroupBuilder(groupId);
        groupSpec.accept(group);
        List<StepDefinition> steps = group.build();
        if (steps.size() < 2) {
            throw new IllegalArgumentException("parallel group '" + groupId + "' needs at least two steps");
        }
        plan.add(new SagaDefinition.GroupItem(groupId, steps));
        return this;
    }

    public SagaDefinition build() {
        if (plan.isEmpty()) {
            throw new IllegalStateException("saga '" + name + "' has no steps");
        }
        return new SagaDefinition(name, plan);
    }

    private void checkUnique(String stepName) {
        if (!stepNames.add(stepName)) {
            throw new IllegalArgumentException("duplicate step name '" + stepName + "' in saga '" + name + "'");
        }
    }

    public final class ParallelGroupBuilder {
        private final String groupId;
        private final List<StepDefinition> steps = new ArrayList<>();

        private ParallelGroupBuilder(String groupId) {
            this.groupId = groupId;
        }

        public ParallelGroupBuilder step(String stepName, SagaAction forward, SagaAction compensation) {
            return step(StepDefinition.of(stepName, forward, compensation).build());
        }

        public ParallelGroupBuilder step(StepDefinition step) {
            checkUnique(step.getName());
            steps.add(step.inGroup(groupId));
            return this;
        }

        private List<StepDefinition> build() {
            return steps;
        }
    }
}
