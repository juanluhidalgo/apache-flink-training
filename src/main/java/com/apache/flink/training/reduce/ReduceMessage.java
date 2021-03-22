package com.apache.flink.training.reduce;

import org.apache.flink.api.common.functions.ReduceFunction;

public class ReduceMessage implements ReduceFunction<String> {

    @Override
    public String reduce(String s, String t1) throws Exception {
        return s + "---" + t1;
    }
}
