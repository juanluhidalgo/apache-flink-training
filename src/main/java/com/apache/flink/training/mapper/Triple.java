package com.apache.flink.training.mapper;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.util.Collector;

public class Triple implements MapPartitionFunction<Integer, Integer> {

    @Override
    public void mapPartition(Iterable<Integer> iterable, Collector<Integer> collector) throws Exception {
        for (Integer number : iterable) {
            collector.collect(number * 3);
        }
    }
}
