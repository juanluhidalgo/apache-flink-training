package com.apache.flink.training.watermark;

import com.apache.flink.training.model.EventMessage;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

public class MessageWatermark implements WatermarkGenerator<EventMessage> {

    private long currentMaxTimestamp;

    @Override
    public void onEvent(EventMessage s, long eventTimestamp, WatermarkOutput watermarkOutput) {
        currentMaxTimestamp = s.getTimestamp();
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
        watermarkOutput.emitWatermark(new Watermark(currentMaxTimestamp));
    }
}
