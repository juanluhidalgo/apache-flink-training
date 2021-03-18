package com.apache.flink.training.aggregate;

import com.apache.flink.training.model.GoalTeam;
import java.util.Objects;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class CountConsecutiveGoals extends RichFlatMapFunction<GoalTeam, GoalTeam> {

    private transient ValueState<GoalTeam> totalGoalsPerTeam;

    @Override
    public void flatMap(GoalTeam goalTeam, Collector<GoalTeam> collector) throws Exception {

        GoalTeam currentGoalTeam = totalGoalsPerTeam.value();

        if (Objects.nonNull(currentGoalTeam)) {
            currentGoalTeam.setGoals(currentGoalTeam.getGoals() + goalTeam.getGoals());
        } else {
            currentGoalTeam = goalTeam;
        }

        totalGoalsPerTeam.update(currentGoalTeam);

        if (goalTeam.getGoals() == 0) {
            collector.collect(currentGoalTeam);
            totalGoalsPerTeam.clear();
        }

    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<GoalTeam> descriptor =
                new ValueStateDescriptor<>(
                        "goals",
                        TypeInformation.of(new TypeHint<GoalTeam>() {
                        }));
        totalGoalsPerTeam = getRuntimeContext().getState(descriptor);
    }
}
