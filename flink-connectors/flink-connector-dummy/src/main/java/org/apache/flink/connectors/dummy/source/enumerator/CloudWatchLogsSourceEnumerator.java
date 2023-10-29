package org.apache.flink.connectors.dummy.source.enumerator;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.connectors.dummy.source.CloudWatchLogsSplit;
import org.apache.flink.util.FlinkRuntimeException;

import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogStream;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class CloudWatchLogsSourceEnumerator implements SplitEnumerator<CloudWatchLogsSplit, CloudWatchLogsEnumState> {
    private final String logGroup;
    private final String logStreamPrefix;
    private final CloudWatchLogsClient logsClient;
    private final SplitEnumeratorContext<CloudWatchLogsSplit> context;

    private List<String> assignedSplits = new ArrayList<>();
    private List<String> unassignedSplits = new ArrayList<>();

    private final CloudWatchLogsSplitAssigner splitAssigner;


    public CloudWatchLogsSourceEnumerator(
            String logGroup,
            String logStreamPrefix,
            SplitEnumeratorContext<CloudWatchLogsSplit> context) {
        this.logGroup = logGroup;
        this.logStreamPrefix = logStreamPrefix;
        this.context = context;
        this.logsClient = CloudWatchLogsClient.builder()
                .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
                .region(Region.US_EAST_1)
                .build();
        this.splitAssigner = new CloudWatchLogsSplitAssigner(context.registeredReaders().size());
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        //TODO DAY3
    }

    @Override
    public void start() {
        // TODO make period configurable
        context.callAsync(this::getStreamsInGroup, this::handleLogStreamUpdate, 0, 30_000);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // the cloudwatch source pushes splits eagerly, rather than act upon split requests
    }

    @Override
    public void addSplitsBack(List<CloudWatchLogsSplit> splits, int subtaskId) {
        // TODO
    }

    @Override
    public void addReader(int subtaskId) {
        splitAssigner.addReader(subtaskId);
    }

    @Override
    public CloudWatchLogsEnumState snapshotState(long checkpointId) throws Exception {
        //TODO DAY3
        return null;
    }

    @Override
    public void close() throws IOException {
        logsClient.close();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        //TODO: DAY3
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        // TODO: figure out if needed
    }

    private Set<LogStream> getStreamsInGroup() {
        DescribeLogStreamsRequest request = DescribeLogStreamsRequest.builder()
                .logGroupName(logGroup)
                .logStreamNamePrefix(logStreamPrefix)
                .build();
        return new HashSet<>(logsClient.describeLogStreams(request).logStreams());
    }

    private void handleLogStreamUpdate(Set<LogStream> logStreams, Throwable t) {
        if(t != null) {
            throw new FlinkRuntimeException("Failed to get log streams", t);
        }
        Set<String> logStreamMap = logStreams.stream().map(LogStream::logStreamName).collect(
                Collectors.toSet());
        assignedSplits.removeIf(split -> !logStreamMap.contains(split));
        unassignedSplits.removeIf(split -> !logStreamMap.contains(split));

        Map<CloudWatchLogsSplit, Integer> newSplits = logStreams.stream()
                .filter(logStream -> !assignedSplits.contains(logStream.logStreamName()) && !unassignedSplits.contains(logStream.logStreamName()))
                .map(ls -> new CloudWatchLogsSplit(logGroup, ls.logStreamName(), ls.firstEventTimestamp() - 60_000))
                .collect(Collectors.toMap(split -> split, splitAssigner::assignSplit));
        Map<Integer, List<CloudWatchLogsSplit>> newAssignment = new HashMap<>();
        if(newSplits.isEmpty())
            return;
        for(Integer reader: context.registeredReaders().keySet()) {
            newAssignment.put(reader, new ArrayList<>());
            newAssignment.get(reader)
                    .addAll(newSplits.entrySet().stream()
                            .filter(es -> es.getValue().equals(reader))
                            .map(Map.Entry::getKey).collect(Collectors.toList()));
            if(newAssignment.get(reader).isEmpty()) {
                newAssignment.remove(reader);
            }
        }
        assignedSplits.addAll(newSplits.keySet().stream().map(CloudWatchLogsSplit::splitId).collect(Collectors.toList()));
        context.assignSplits(new SplitsAssignment<>(newAssignment));
    }
}
