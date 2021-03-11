package com.apache.flink.training.filter;

import org.apache.flink.api.common.functions.FilterFunction;

public class JustEven implements FilterFunction<Integer> {

    @Override
    public boolean filter(Integer number) throws Exception {
        return number % 2 == 0;
    }
}
