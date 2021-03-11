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

public class BatchTransformationsSample {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        Collection<Integer> numbers = Arrays.asList(1,
                                                    2,
                                                    3,
                                                    4);

        DataSet<Integer> repeatedDouble = env.fromCollection(numbers).name("Collection of Numbers").filter(new JustEven())
                .name("Filtering")
                .flatMap(new RepeatDouble()).name("Repeat Double");

        DataSet<Integer> triple = env.fromCollection(numbers).name("Collection of Numbers").filter(new JustEven())
                .name("Filtering")
                .mapPartition(new Triple()).name("Triple");

        DataSet<Integer> nonRepeatedDouble = env.fromCollection(numbers).name("Collection of Numbers").filter(new JustEven())
                .name("Filtering").map(new Double()).name("Double");

        repeatedDouble.union(nonRepeatedDouble).union(triple)
                .output(new PrintingOutputFormat<>()).name("Writing");
        env.execute("Batch App");
    }
}
