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


import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.StatefulSink;

import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CloudWatchLogsSinkWriter<InputT> implements StatefulSink.StatefulSinkWriter<InputT, CloudWatchLogsSinkState> {

    private final MailboxExecutor executor;
    private final ProcessingTimeService processingTimeService;
    private final Integer subtaskId;
    private final CloudWatchLogsSink.LogEventTransformer<InputT> eventTransformer;
    private final CloudWatchLogsSink.LogEventStreamTransformer<InputT> logEventStreamTransformer;
    private final String logGroup;
    private final CloudWatchLogsClient logsClient;
    private Map<String, List<InputLogEvent>> logEventBatchMap = new HashMap<>();
    public CloudWatchLogsSinkWriter(String logGroup,
                                    MailboxExecutor executor,
                                    ProcessingTimeService timeService,
                                    Integer subtaskId,
                                    CloudWatchLogsSink.LogEventTransformer<InputT> eventTransformer,
                                    CloudWatchLogsSink.LogEventStreamTransformer<InputT> logEventStreamTransformer,
                                    Collection<CloudWatchLogsSinkState> states
    ) {

        this.logGroup = logGroup;
        this.executor = executor;
        this.processingTimeService = timeService;
        this.subtaskId = subtaskId;
        this.eventTransformer = eventTransformer;
        this.logEventStreamTransformer = logEventStreamTransformer;
        this.logsClient = CloudWatchLogsClient.builder()
                .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
                .build();
        for(CloudWatchLogsSinkState state : states) {
            logEventBatchMap.put(state.streamName, state.streamContent);
        }
    }

    @Override
    public void write(InputT element, Context context) throws IOException, InterruptedException {

        String streamName = logEventStreamTransformer.extractLogEventStream(element);
        if(!logEventBatchMap.containsKey(streamName)) {
            logEventBatchMap.put(streamName, new ArrayList<>());
        }
        logEventBatchMap.get(streamName).add(eventTransformer.extractLogEvent(element));
        if(logEventBatchMap.get(streamName).size() >= 50) {
            flushStream(streamName);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        for(String streamName : logEventBatchMap.keySet()) {
            flushStream(streamName);
        }
    }

    private void flushStream(String streamName) {
        DescribeLogStreamsRequest logStreamRequest = DescribeLogStreamsRequest.builder()
                .logGroupName(logGroup)
                .logStreamNamePrefix(streamName)
                .build();

        DescribeLogStreamsResponse describeLogStreamsResponse = logsClient.describeLogStreams(logStreamRequest);

        // Assume that a single stream is returned since a specific stream name was specified in the previous request.
        String sequenceToken = describeLogStreamsResponse.logStreams().get(0).uploadSequenceToken();
        PutLogEventsRequest putLogEventsRequest = PutLogEventsRequest.builder()
                .logEvents(logEventBatchMap.get(streamName))
                .logGroupName(logGroup)
                .logStreamName(streamName)
                .sequenceToken(sequenceToken)
                .build();
        try {
            logsClient.putLogEvents(putLogEventsRequest);
            logEventBatchMap.remove(streamName);
        } catch (Exception e) {
            System.out.println("Don't take it that seriously bro " + e.getMessage());
        }
    }

    @Override
    public void close() throws Exception {
        this.logsClient.close();
    }

    @Override
    public List<CloudWatchLogsSinkState> snapshotState(long checkpointId) throws IOException {
        List<CloudWatchLogsSinkState> states = new ArrayList<>();
        for(String streamName: logEventBatchMap.keySet()) {
            states.add(new CloudWatchLogsSinkState(streamName, logEventBatchMap.get(streamName)));
        }
        return states;
    }
}
