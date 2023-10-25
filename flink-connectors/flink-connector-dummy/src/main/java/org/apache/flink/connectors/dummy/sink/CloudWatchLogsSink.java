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

package org.apache.flink.connectors.dummy.sink;

import org.apache.flink.api.connector.sink2.StatefulSink;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

public class CloudWatchLogsSink<Input> implements StatefulSink<Input, CloudWatchLogsSinkState> {
    private final LogEventStreamTransformer<Input> logStreamExtractor;
    private final LogEventTransformer<Input> transformer;
    private final String logGroup;

    public CloudWatchLogsSink(String logGroup, LogEventStreamTransformer<Input> logStreamExtractor, LogEventTransformer<Input> transformer) {
        this.logGroup = logGroup;
        this.logStreamExtractor = logStreamExtractor;
        this.transformer = transformer;
    }

    @Override
    public StatefulSinkWriter<Input, CloudWatchLogsSinkState> createWriter(InitContext context) throws IOException {
        return new CloudWatchLogsSinkWriter<Input>(
                logGroup,
                context.getMailboxExecutor(),
                context.getProcessingTimeService(),
                context.getSubtaskId(),
                transformer,
                logStreamExtractor,
                Collections.emptyList());
    }

    @Override
    public StatefulSinkWriter<Input, CloudWatchLogsSinkState> restoreWriter(
            InitContext context,
            Collection<CloudWatchLogsSinkState> recoveredState) throws IOException {
        return new CloudWatchLogsSinkWriter<Input>(
                logGroup,
                context.getMailboxExecutor(),
                context.getProcessingTimeService(),
                context.getSubtaskId(),
                transformer,
                logStreamExtractor,
                recoveredState);
    }

    @Override
    public SimpleVersionedSerializer<CloudWatchLogsSinkState> getWriterStateSerializer() {
        return new CloudWatchLogsSinkState.CloudWatchLogsSinkStateSerializer();
    }

    @FunctionalInterface
    public interface LogEventTransformer<Input> extends Serializable {
        InputLogEvent extractLogEvent(Input input);
    }

    @FunctionalInterface
    public interface LogEventStreamTransformer<Input> extends Serializable {
        String extractLogEventStream(Input input);
    }
}
