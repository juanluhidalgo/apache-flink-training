package com.apache.flink.training;

import com.apache.flink.training.deserializer.EventMessageDeserializationSchema;
import com.apache.flink.training.model.EventMessage;
import com.apache.flink.training.pattern.FromEventMessageToAlert;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class StreamKafkaCEPSample {

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

        DataStream<EventMessage> eventMessages = env.addSource(new FlinkKafkaConsumer<>("myInputTopic",
                                                                                        new EventMessageDeserializationSchema(),
                                                                                        properties)).name("Read messages");

        Pattern<EventMessage, ?> pattern = Pattern.<EventMessage>begin("first").where(
                new SimpleCondition<EventMessage>() {
                    @Override
                    public boolean filter(EventMessage event) {
                        return "Alert".equals(event.getMessage());
                    }
                }
        ).times(3).within(Time.seconds(3));

        PatternStream<EventMessage> patternStream = CEP.pattern(eventMessages,
                                                                pattern).inProcessingTime();

        patternStream.process(
                new FromEventMessageToAlert()).addSink(new FlinkKafkaProducer<>("myOutputTopic",
                                                                                new SimpleStringSchema(),
                                                                                outputProperties)).name("Produce messages");

        env.disableOperatorChaining();

        env.execute("Streaming App");
    }

}
