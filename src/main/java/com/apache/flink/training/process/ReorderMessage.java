package com.apache.flink.training.process;

import com.apache.flink.training.model.EventMessage;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class ReorderMessage extends ProcessWindowFunction<EventMessage, String, String, TimeWindow> {


    @Override
    public void process(String s, Context context, Iterable<EventMessage> iterable, Collector<String> collector) throws Exception {

        List<EventMessage> sortedList = StreamSupport
                .stream(iterable.spliterator(),
                        false)
                .collect(Collectors.toList());

        Collections.sort(sortedList,
                         Comparator.comparing(EventMessage::getTimestamp));

        for (EventMessage me : sortedList) {
            collector.collect(me.getMessage());
        }
    }
}
