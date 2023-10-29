package org.apache.flink.connectors.dummy.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connectors.dummy.source.enumerator.CloudWatchLogsEnumState;
import org.apache.flink.connectors.dummy.source.enumerator.CloudWatchLogsSourceEnumerator;
import org.apache.flink.connectors.dummy.source.reader.CloudWatchLogsRecordEmitter;
import org.apache.flink.connectors.dummy.source.reader.CloudWatchLogsSourceReader;
import org.apache.flink.connectors.dummy.source.reader.CloudWatchLogsStreamReader;
import org.apache.flink.connectors.dummy.source.reader.fetcher.CloudWatchLogsFetcherManager;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import software.amazon.awssdk.services.cloudwatchlogs.model.OutputLogEvent;

import java.io.IOException;
import java.util.function.Function;

public class CloudWatchLogsSource<T> implements Source<T, CloudWatchLogsSplit, CloudWatchLogsEnumState> {
    private final Function<OutputLogEvent, T> converter;
    private final String logGroup;

    private final String streamPrefixes;

    public CloudWatchLogsSource(Function<OutputLogEvent, T> converter, String logGroup,
                               String streamPrefixes) {
        this.converter = converter;
        this.logGroup = logGroup;
        this.streamPrefixes = streamPrefixes;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    // TODO
    @Override
    public SplitEnumerator<CloudWatchLogsSplit, CloudWatchLogsEnumState> createEnumerator(
            SplitEnumeratorContext<CloudWatchLogsSplit> enumContext) throws Exception {
        return new CloudWatchLogsSourceEnumerator(logGroup, streamPrefixes, enumContext);
    }

    // TODO DAY3
    @Override
    public SplitEnumerator<CloudWatchLogsSplit, CloudWatchLogsEnumState> restoreEnumerator(
            SplitEnumeratorContext<CloudWatchLogsSplit> enumContext,
            CloudWatchLogsEnumState checkpoint) throws Exception {
        return null;
    }

    // TODO
    @Override
    public SimpleVersionedSerializer<CloudWatchLogsSplit> getSplitSerializer() {
        return new CloudWatchLogsSplitSerializer(logGroup);
    }

    // TODO
    @Override
    public SimpleVersionedSerializer<CloudWatchLogsEnumState> getEnumeratorCheckpointSerializer() {
        return new SimpleVersionedSerializer<CloudWatchLogsEnumState>() {
            @Override
            public int getVersion() {
                return 0;
            }

            @Override
            public byte[] serialize(CloudWatchLogsEnumState obj) throws IOException {
                return new byte[0];
            }

            @Override
            public CloudWatchLogsEnumState deserialize(
                    int version,
                    byte[] serialized) throws IOException {
                return null;
            }
        };
    }

    @Override
    public SourceReader<T, CloudWatchLogsSplit> createReader(SourceReaderContext readerContext) throws Exception {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<OutputLogEvent>> eq = new FutureCompletingBlockingQueue<>();
        return new CloudWatchLogsSourceReader<>(eq,
                new CloudWatchLogsFetcherManager(eq, () -> new CloudWatchLogsStreamReader(logGroup), new Configuration()),
                new CloudWatchLogsRecordEmitter<>(converter),
                new Configuration(),
                readerContext);
    }
}
