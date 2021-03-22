package com.apache.flink.training.watermark;

import com.apache.flink.training.model.EventMessage;
import java.awt.Event;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier.Context;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

public class MessageWatermarkStrategy implements WatermarkStrategy<EventMessage> {

    @Override
    public WatermarkGenerator<EventMessage> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new MessageWatermark();
    }

    @Override
    public TimestampAssigner<EventMessage> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new TimestampMessageAssigner();
    }
}
