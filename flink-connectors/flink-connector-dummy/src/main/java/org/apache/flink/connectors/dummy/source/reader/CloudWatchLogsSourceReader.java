package org.apache.flink.connectors.dummy.source.reader;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordEvaluator;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import org.apache.flink.connectors.dummy.source.CloudWatchLogsSplit;
import org.apache.flink.connectors.dummy.source.CloudWatchSourceSplitState;

import software.amazon.awssdk.services.cloudwatchlogs.model.OutputLogEvent;

import javax.annotation.Nullable;

import java.util.Map;

public class CloudWatchLogsSourceReader<T> extends SingleThreadMultiplexSourceReaderBase<OutputLogEvent, T, CloudWatchLogsSplit, CloudWatchSourceSplitState> {

    private final SourceReaderContext context;

    public CloudWatchLogsSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<OutputLogEvent>> elementsQueue,
            SingleThreadFetcherManager<OutputLogEvent, CloudWatchLogsSplit> splitFetcherManager,
            RecordEmitter<OutputLogEvent, T, CloudWatchSourceSplitState> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        super(elementsQueue, splitFetcherManager, recordEmitter, config, context);
        this.context = context;
    }

    public CloudWatchLogsSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<OutputLogEvent>> elementsQueue,
            SingleThreadFetcherManager<OutputLogEvent, CloudWatchLogsSplit> splitFetcherManager,
            RecordEmitter<OutputLogEvent, T, CloudWatchSourceSplitState> recordEmitter,
            @Nullable RecordEvaluator<T> eofRecordEvaluator,
            Configuration config,
            SourceReaderContext context) {
        super(
                elementsQueue,
                splitFetcherManager,
                recordEmitter,
                eofRecordEvaluator,
                config,
                context);
        this.context = context;
    }

    @Override
    protected void onSplitFinished(Map<String, CloudWatchSourceSplitState> finishedSplitIds) {

    }

    @Override
    protected CloudWatchSourceSplitState initializedState(CloudWatchLogsSplit split) {
        return null;
    }

    @Override
    protected CloudWatchLogsSplit toSplitType(
            String splitId,
            CloudWatchSourceSplitState splitState) {
        return null;
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        super.notifyCheckpointAborted(checkpointId);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
    }
}
