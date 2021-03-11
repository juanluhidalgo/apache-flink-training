package com.apache.flink.training.reduce;

import com.apache.flink.training.model.GoalTeam;
import org.apache.flink.api.java.functions.KeySelector;

public class SelectTeam implements KeySelector<GoalTeam, String> {

    @Override
    public String getKey(GoalTeam footballTeam) throws Exception {
        return footballTeam.getTeamName();
    }
}
