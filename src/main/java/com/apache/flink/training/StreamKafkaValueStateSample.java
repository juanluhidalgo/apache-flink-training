package com.apache.flink.training;

import com.apache.flink.training.aggregate.CountConsecutiveGoals;
import com.apache.flink.training.mapper.FromStringToGoalTeam;
import com.apache.flink.training.model.GoalTeam;
import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class StreamKafkaValueStateSample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers",
                               "localhost:9092");
        properties.setProperty("group.id",
                               "test");

        Properties outputProperties = new Properties();

        outputProperties.setProperty("bootstrap.servers",
                                     "localhost:9092");

        DataStream<String> messages = env.addSource(new FlinkKafkaConsumer<>("myInputTopic",
                                                                             new SimpleStringSchema(),
                                                                             properties)).name("Read messages");

        messages.addSink(new PrintSinkFunction<>());

        DataStream<GoalTeam> goals = messages.map(new FromStringToGoalTeam());

        goals.keyBy(a -> a.getTeamName()).flatMap(new CountConsecutiveGoals())
                .map(goalTeam -> new ObjectMapper().writeValueAsString(goalTeam)).addSink(new FlinkKafkaProducer<>("myOutputTopic",
                                                                                                                   new SimpleStringSchema(),
                                                                                                                   outputProperties))
                .name("Produce messages");

        env.disableOperatorChaining();

        env.execute("Streaming App");
    }
}
