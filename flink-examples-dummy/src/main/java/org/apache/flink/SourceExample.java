package org.apache.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connectors.dummy.source.CloudWatchLogsSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.services.cloudwatchlogs.model.OutputLogEvent;

import java.io.Serializable;
import java.time.Instant;
import java.util.Date;
import java.util.function.Function;

public class SourceExample {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
//        env.enableCheckpointing(1000, CheckpointingMode.AT_LEAST_ONCE);
        CloudWatchLogsSource<StringWithTime> source = new CloudWatchLogsSource<>(new Convertor(), "flinkConnectorTest", "connectorTest");
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "SingleSource", TypeInformation.of(StringWithTime.class))
                .sinkTo(new PrintSink<>());
        env.execute("Yallaa B2a");
    }

    public static class Convertor implements Serializable , Function<OutputLogEvent, StringWithTime> {

        @Override
        public StringWithTime apply(OutputLogEvent outputLogEvent) {
            return new StringWithTime(
                    outputLogEvent.message(), outputLogEvent.timestamp());
        }
    }

    public static class StringWithTime {
        public final String message;
        public final Long timestamp;
        public StringWithTime(String message, Long timestamp){

            this.message = message;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return message + " " + Date.from(Instant.ofEpochMilli(timestamp));
        }
    }
}
