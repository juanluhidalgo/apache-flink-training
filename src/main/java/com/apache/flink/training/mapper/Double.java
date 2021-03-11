package com.apache.flink.training.mapper;

import org.apache.flink.api.common.functions.MapFunction;

public class Double implements MapFunction<Integer, Integer> {

    @Override
    public Integer map(Integer number) throws Exception {
        return number * 2;
    }
}
