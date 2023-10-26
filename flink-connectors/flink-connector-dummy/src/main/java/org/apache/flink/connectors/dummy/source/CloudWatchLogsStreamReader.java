package org.apache.flink.connectors.dummy.source;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;

import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.OutputLogEvent;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

public class CloudWatchLogsStreamReader implements SplitReader<OutputLogEvent, CloudWatchLogsSplit> {
    private final CloudWatchLogsClient logsClient;

    public CloudWatchLogsStreamReader() {
        this.logsClient = CloudWatchLogsClient.builder()
                .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
                .build();
    }

    @Override
    public RecordsWithSplitIds<OutputLogEvent> fetch() throws IOException {

        // TODO
        GetLogEventsRequest request = GetLogEventsRequest.builder().build();
        GetLogEventsResponse response = this.logsClient.getLogEvents(request);
        Iterator<OutputLogEvent> eventIterator = response.events().iterator();

        return new RecordsWithSplitIds<OutputLogEvent>() {
            @Nullable
            @Override
            public String nextSplit() {
                return "SreamName";
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

    }

    @Override
    public void wakeUp() {

    }

    @Override
    public void close() throws Exception {

    }
}
