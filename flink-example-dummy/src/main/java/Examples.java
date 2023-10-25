/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.connectors.dummy.sink.CloudWatchLogsSink;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.utils.ImmutableMap;

import java.time.Instant;

public class Examples {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String []args) throws Exception {
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
        env.execute("Yallaa");

    }
}
