package com.apache.flink.training;

import com.apache.flink.training.filter.JustEven;
import com.apache.flink.training.mapper.Double;
import com.apache.flink.training.mapper.RepeatDouble;
import com.apache.flink.training.mapper.Triple;
import java.util.Arrays;
import java.util.Collection;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.PrintingOutputFormat;
import org.apache.flink.api.java.operators.IterativeDataSet;

public class BatchIterationSample {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        Collection<Integer> numbers = Arrays.asList(1,
                                                    2,
                                                    3,
                                                    4);

        IterativeDataSet<Integer> initial = env.fromCollection(numbers).iterate(10);
        DataSet<Integer> repeatedDouble = initial.map(new Double());
        initial.closeWith(repeatedDouble).output(new PrintingOutputFormat<>()).name("Writing");
        env.execute("Batch App");
    }
}
