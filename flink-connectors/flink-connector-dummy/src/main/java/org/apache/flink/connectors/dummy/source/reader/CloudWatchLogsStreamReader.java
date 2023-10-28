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

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class CloudWatchLogsStreamReader implements SplitReader<OutputLogEvent, CloudWatchLogsSplit> {
    private final String logGroup;
    private final CloudWatchLogsClient logsClient;

    private Set<CloudWatchLogsSplit> streamsToReadFrom;

    public CloudWatchLogsStreamReader(String logGroup) {
        this.logGroup = logGroup;
        this.logsClient = CloudWatchLogsClient.builder()
                .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
                .build();
        this.streamsToReadFrom = new HashSet<>();
    }

    @Override
    public RecordsWithSplitIds<OutputLogEvent> fetch() throws IOException {

        // TODO
        GetLogEventsRequest request = GetLogEventsRequest.builder()
                .logGroupName(logGroup)
                .build();
        GetLogEventsResponse response = this.logsClient.getLogEvents(request);
        Iterator<OutputLogEvent> eventIterator = response.events().iterator();

        return new RecordsWithSplitIds<OutputLogEvent>() {
            @Nullable
            @Override
            public String nextSplit() {
                return "";
            }

            @Nullable
            @Override
            public OutputLogEvent nextRecordFromSplit() {
                return eventIterator.next();
            }

            @Override
            public Set<String> finishedSplits() {
                return Collections.emptySet();
            }
        };
    }

    @Override
    public void handleSplitsChanges(SplitsChange<CloudWatchLogsSplit> splitsChanges) {
        if (splitsChanges instanceof SplitsAddition) {
            streamsToReadFrom.addAll(splitsChanges.splits());
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
