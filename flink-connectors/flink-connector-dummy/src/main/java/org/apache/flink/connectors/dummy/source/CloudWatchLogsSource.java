package org.apache.flink.connectors.dummy.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import software.amazon.awssdk.services.cloudwatchlogs.model.OutputLogEvent;

import java.util.function.Function;

public class CloudWatchLogsSource<T> implements Source<T, CloudWatchLogsSplit, CloudWatchSourceSplitState> {
    private final Function<OutputLogEvent, T> converter;

    public CloudWatchLogsSource(Function<OutputLogEvent, T> converter) {
        this.converter = converter;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<CloudWatchLogsSplit, CloudWatchSourceSplitState> createEnumerator(
            SplitEnumeratorContext<CloudWatchLogsSplit> enumContext) throws Exception {
        return null;
    }

    @Override
    public SplitEnumerator<CloudWatchLogsSplit, CloudWatchSourceSplitState> restoreEnumerator(
            SplitEnumeratorContext<CloudWatchLogsSplit> enumContext,
            CloudWatchSourceSplitState checkpoint) throws Exception {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<CloudWatchLogsSplit> getSplitSerializer() {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<CloudWatchSourceSplitState> getEnumeratorCheckpointSerializer() {
        return null;
    }

    @Override
    public SourceReader<T, CloudWatchLogsSplit> createReader(SourceReaderContext readerContext) throws Exception {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<OutputLogEvent>> eq = new FutureCompletingBlockingQueue<>();
        return new CloudWatchLogsSourceReader<>(eq, new CloudWatchLogsFetcherManager(eq,
                CloudWatchLogsStreamReader::new ,new Configuration(),(ignore) -> {}), new CloudWatchLogsRecordEmitter<T>(converter),new Configuration(), readerContext);
    }
}
