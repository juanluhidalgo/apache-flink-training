package com.apache.flink.training.aggregate;

import com.apache.flink.training.model.GoalTeam;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class ConsecutiveGoalsList extends RichFlatMapFunction<GoalTeam, GoalTeam> {

    private transient ListState<GoalTeam> totalGoalsPerTeam;

    @Override
    public void flatMap(GoalTeam goalTeam, Collector<GoalTeam> collector) throws Exception {

        totalGoalsPerTeam.add(goalTeam);

        if (goalTeam.getGoals() == 0) {
            for(GoalTeam gT : totalGoalsPerTeam.get()){
                collector.collect(gT);
                totalGoalsPerTeam.clear();
            }
        }
    }

    @Override
    public void open(Configuration config) {
        ListStateDescriptor<GoalTeam> descriptor =
                new ListStateDescriptor<>(
                        "goals",
                        TypeInformation.of(new TypeHint<GoalTeam>() {
                        }));
        totalGoalsPerTeam = getRuntimeContext().getListState(descriptor);
    }
}
