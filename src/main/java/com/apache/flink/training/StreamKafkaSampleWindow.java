package com.apache.flink.training;

import com.apache.flink.training.mapper.FromStringToEventMessage;
import com.apache.flink.training.mapper.FromStringToGoalTeam;
import com.apache.flink.training.model.EventMessage;
import com.apache.flink.training.process.Echo;
import com.apache.flink.training.process.ReorderMessage;
import com.apache.flink.training.reduce.ReduceMessage;
import com.apache.flink.training.watermark.MessageWatermark;
import com.apache.flink.training.watermark.MessageWatermarkStrategy;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class StreamKafkaSampleWindow {

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

        DataStream<EventMessage> messages = env.addSource(new FlinkKafkaConsumer<>("myInputTopic",
                                                                                   new SimpleStringSchema(),
                                                                                   properties)).name("Read messages")
                .map(new FromStringToEventMessage());

        DataStream<EventMessage> messagesWithWatermark = messages
                .assignTimestampsAndWatermarks(new MessageWatermarkStrategy());

        messagesWithWatermark.keyBy(m -> "1").window(TumblingEventTimeWindows.of(Time.seconds(5))).allowedLateness(Time.seconds(5))
                .process(new ReorderMessage())
                .addSink(new FlinkKafkaProducer<>("myOutputTopic",
                                                  new SimpleStringSchema(),
                                                  outputProperties)).name("Produce messages");

        env.disableOperatorChaining();

        env.execute("Streaming App");
    }
}
