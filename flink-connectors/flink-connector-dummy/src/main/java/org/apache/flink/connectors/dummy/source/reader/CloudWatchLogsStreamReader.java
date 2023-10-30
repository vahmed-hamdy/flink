package org.apache.flink.connectors.dummy.source.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;

import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import org.apache.flink.connectors.dummy.source.CloudWatchLogsSplit;

import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.OutputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceNotFoundException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CloudWatchLogsStreamReader implements SplitReader<OutputLogEvent, CloudWatchLogsSplit> {
    private final String logGroup;
    private final CloudWatchLogsClient logsClient;

    private final List<CloudWatchLogsSplit> streamsToReadFrom;

    public CloudWatchLogsStreamReader(String logGroup) {
        this.logGroup = logGroup;
        this.logsClient = CloudWatchLogsClient.builder()
                .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
                .build();
        this.streamsToReadFrom = new ArrayList<>();
    }

    @Override
    public RecordsWithSplitIds<OutputLogEvent> fetch() throws IOException {

        /// We are assuming a single stream per reader.
        if(streamsToReadFrom.isEmpty()) {
            return null;
        }
        CloudWatchLogsSplit split = streamsToReadFrom.get(0);
        boolean isFinished = false;
        // TODO support NextToken
        GetLogEventsRequest request = GetLogEventsRequest.builder()
                .logGroupName(logGroup)
                .logStreamName(split.splitId())
                .startTime(split.getStartTimeStamp())
                .endTime(Instant.now().toEpochMilli())
                .startFromHead(true)
                .limit(100) // TODO: configure
                .build();
        List<OutputLogEvent> logEvents = new ArrayList<>();
        try {
            GetLogEventsResponse response = this.logsClient.getLogEvents(request);

            split.setStartTimeStamp(response.events().stream().map(OutputLogEvent::timestamp).max(Long::compare).orElse(split.getStartTimeStamp()) + 1);
            logEvents = response.events();
        } catch (ResourceNotFoundException ignored) {
            isFinished = true;
        }
        Iterator<OutputLogEvent> eventIterator = logEvents.iterator();

        boolean finalIsFinished = isFinished;
        return new RecordsWithSplitIds<OutputLogEvent>() {
            @Nullable
            @Override
            public String nextSplit() {
                return eventIterator.hasNext() ? split.splitId() : null;
            }

            @Nullable
            @Override
            public OutputLogEvent nextRecordFromSplit() {
                if(!eventIterator.hasNext())
                    return null;
                OutputLogEvent event = eventIterator.next();

                return  OutputLogEvent.builder().message("{Stream: " + split.splitId()+"} + " + event.message()).timestamp(event.timestamp()).build();
            }

            // Currently we do not support finished log streams
            @Override
            public Set<String> finishedSplits() {
                return finalIsFinished ? Collections.singleton(split.splitId()) : Collections.emptySet();
            }
        };
    }

    @Override
    public void handleSplitsChanges(SplitsChange<CloudWatchLogsSplit> splitsChanges) {
        if (splitsChanges instanceof SplitsAddition) {
            if(splitsChanges.splits().size() != 1) {
                throw new IllegalStateException("a7a maynfa3sh");
            }
            streamsToReadFrom.addAll(splitsChanges.splits());

        } else {
            streamsToReadFrom.clear();
        }
    }

    @Override
    public void wakeUp() {
        // DO nothing
    }

    @Override
    public void close() throws Exception {
        logsClient.close();
    }
}
