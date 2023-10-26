package org.apache.flink;

import org.apache.flink.connectors.dummy.sink.CloudWatchLogsSink;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;

import java.time.Instant;

public class Main {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000, CheckpointingMode.AT_LEAST_ONCE);

        DataStream<String> fromGen =
                env.fromSequence(1, 10_000_000L)
                        .map(Object::toString)
                        .returns(String.class)
                        .map(
                                data ->
                                        OBJECT_MAPPER.writeValueAsString(
                                                ImmutableMap.of("data", data)));

        CloudWatchLogsSink<String> sink = new CloudWatchLogsSink<String>("flinkConnectorTest",
                (s -> "connectorTestStream"),
                s -> InputLogEvent.builder().timestamp(Instant.now().toEpochMilli()).message(s).build());
        fromGen.sinkTo(sink);
//        fromGen.sinkTo(new PrintSink<>());
        env.execute("Yallaa");
    }
}
