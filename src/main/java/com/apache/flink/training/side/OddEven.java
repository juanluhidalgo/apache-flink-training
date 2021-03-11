package com.apache.flink.training.side;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class OddEven extends ProcessFunction<Integer, Integer> {

    final OutputTag<String> outputTag = new OutputTag<String>("odd") {
    };


    @Override
    public void processElement(Integer integer, Context context, Collector<Integer> collector) throws Exception {
        if (integer % 2 == 0) {
            collector.collect(integer);
        } else {
            context.output(outputTag,
                           String.valueOf(integer));
        }
    }
}
