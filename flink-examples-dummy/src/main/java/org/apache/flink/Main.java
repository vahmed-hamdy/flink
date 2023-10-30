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
//        env.enableCheckpointing(1000, CheckpointingMode.AT_LEAST_ONCE);
        DataStream<Long> fromGen =
                env.fromSequence(1, 10_000_000L);
        CloudWatchLogsSink<String> sink1= new CloudWatchLogsSink<String>("flinkConnectorTest",
                (s -> "connectorTestLogStream"),
                s -> InputLogEvent.builder().timestamp(Instant.now().toEpochMilli()).message(s).build());

        CloudWatchLogsSink<String> sink2 = new CloudWatchLogsSink<String>("flinkConnectorTest",
                (s -> "connectorTesStream"),
                s -> InputLogEvent.builder().timestamp(Instant.now().toEpochMilli()).message(s).build());
        fromGen.filter(l -> l.longValue() %2 == 0)
                .map(Object::toString)
                .returns(String.class)
                .map(
                        data ->
                                OBJECT_MAPPER.writeValueAsString(
                                        ImmutableMap.of("data", data))).sinkTo(sink1);

        fromGen.filter(l -> l.longValue() %2 == 1)
                .map(Object::toString)
                .returns(String.class)
                .map(
                        data ->
                                OBJECT_MAPPER.writeValueAsString(
                                        ImmutableMap.of("data", data))).sinkTo(sink2);
//        fromGen.sinkTo(new PrintSink<>());
        env.execute("Yallaa");
    }
}
