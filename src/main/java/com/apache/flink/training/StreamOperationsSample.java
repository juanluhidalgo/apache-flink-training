package com.apache.flink.training;

import com.apache.flink.training.filter.JustEven;
import com.apache.flink.training.key.MyKey;
import com.apache.flink.training.mapper.Double;
import com.apache.flink.training.mapper.RepeatDouble;
import com.apache.flink.training.mapper.Triple;
import com.apache.flink.training.reduce.SumNumber;
import com.apache.flink.training.side.OddEven;
import com.apache.flink.training.sink.MySink;
import java.util.Arrays;
import java.util.Collection;
import org.apache.commons.io.output.WriterOutputStream;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.PrintingOutputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.OutputTag;

public class StreamOperationsSample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        Collection<Integer> numbers = Arrays.asList(10,
                                                    2,
                                                    3,
                                                    4,
                                                    7);

        SingleOutputStreamOperator<Integer> oddOrEven = (SingleOutputStreamOperator<Integer>) env.fromCollection(numbers)
                .name("Collection of Numbers")
                .process(new OddEven());



        //numbers.addSink(new PrintSinkFunction<>()).name("Writing");

        //oddOrEven.getSideOutput(new OutputTag<String>("odd"){}).addSink(new PrintSinkFunction<>()).name("Writing");

        oddOrEven.addSink(new MySink());

        env.execute("Streaming App");
    }
}
