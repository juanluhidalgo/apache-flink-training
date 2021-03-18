package com.apache.flink.training.mapper;

import com.apache.flink.training.model.CoachTeam;
import com.apache.flink.training.model.GoalTeam;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class FromStringToGoalTeam implements MapFunction<String, GoalTeam> {


    @Override
    public GoalTeam map(String json) throws Exception {
        return new ObjectMapper().readValue(json,
                                            GoalTeam.class);

    }
}
