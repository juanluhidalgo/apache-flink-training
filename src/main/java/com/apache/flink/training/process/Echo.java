package com.apache.flink.training.process;

import com.apache.flink.training.model.EventMessage;
import org.apache.flink.shaded.akka.org.jboss.netty.channel.MessageEvent;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Echo extends ProcessWindowFunction<EventMessage, String, String, TimeWindow> {


    @Override
    public void process(String s, Context context, Iterable<EventMessage> iterable, Collector<String> collector) throws Exception {
        for (EventMessage me : iterable) {
            collector.collect(me.getMessage());
        }
    }
}
