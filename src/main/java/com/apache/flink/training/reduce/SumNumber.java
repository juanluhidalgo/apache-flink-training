package com.apache.flink.training.reduce;

import org.apache.flink.api.common.functions.ReduceFunction;

public class SumNumber implements ReduceFunction<Integer> {

    @Override
    public Integer reduce(Integer integer, Integer t1) throws Exception {
        return integer * t1;
    }
}
