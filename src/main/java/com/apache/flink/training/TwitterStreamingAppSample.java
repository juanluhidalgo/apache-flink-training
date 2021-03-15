package com.apache.flink.training;

import com.apache.flink.training.mapper.FromTweetToTuple;
import com.apache.flink.training.source.TwitterExampleData;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.OutputTag;

/**
 * Lee los datos usando @TwitterExampleData
 * Obten la palabra más repetida en Ingles
 * Obten la palabra más repetida en idiomas que no sean Ingles
 * Obten el maximo número de palabras en tweets en Ingles y no Ingles
 */

public class TwitterStreamingAppSample {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStream<String> streamSource = env.fromElements(TwitterExampleData.TEXTS);

        SingleOutputStreamOperator<Tuple2<String, Integer>> words = streamSource.process(new FromTweetToTuple());

        SingleOutputStreamOperator<Tuple2<String, Integer>> mostRepeatedWordEnglish = words
                .getSideOutput(new OutputTag<Tuple2<String, Integer>>("no-english") {
                })
                .keyBy(a -> a.f0).max(1);

        // count para palabra más repetida

        mostRepeatedWordEnglish.addSink(new PrintSinkFunction<>("No English", false)).name("No English");
        words.keyBy(a -> a.f0).max(1).addSink(new PrintSinkFunction<>("English", false)).name("English");

        env.execute("Twitter Sample");

    }


}
