package com.apache.flink.training.sink;

import com.apache.flink.training.model.CoachTeam;
import com.apache.flink.training.model.GoalTeam;
import java.io.IOException;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class JsonOutputFormat implements OutputFormat<GoalTeam> {

    @Override
    public void configure(Configuration configuration) {

    }

    @Override
    public void open(int i, int i1) throws IOException {

    }

    @Override
    public void writeRecord(GoalTeam goalTeam) throws IOException {
        System.out.println(new ObjectMapper().writeValueAsString(goalTeam));
    }

    @Override
    public void close() throws IOException {

    }
}
