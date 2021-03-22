package com.apache.flink.training.mapper;

import com.apache.flink.training.model.EventMessage;
import com.apache.flink.training.model.GoalTeam;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class FromStringToEventMessage implements MapFunction<String, EventMessage> {

    @Override
    public EventMessage map(String json) throws Exception {
        return new ObjectMapper().readValue(json,
                                            EventMessage.class);

    }
}
