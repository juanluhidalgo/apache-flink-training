package com.apache.flink.training.mapper;

import com.apache.flink.training.model.CoachTeam;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class FromStringToCoachTeam implements MapFunction<String, CoachTeam> {


    @Override
    public CoachTeam map(String json) throws Exception {
        return new ObjectMapper().readValue(json,
                                            CoachTeam.class);

    }
}
