package com.apache.flink.training.reduce;

import com.apache.flink.training.model.GoalTeam;
import org.apache.flink.api.common.functions.ReduceFunction;

public class GoalCounter implements ReduceFunction<GoalTeam> {

    @Override
    public GoalTeam reduce(GoalTeam footballTeam1, GoalTeam footballTeam2) throws Exception {
        return new GoalTeam(footballTeam1.getTeamName(), footballTeam1.getGoals() + footballTeam2.getGoals());
    }
}
