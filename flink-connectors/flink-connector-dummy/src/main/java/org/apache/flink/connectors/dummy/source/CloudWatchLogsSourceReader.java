package org.apache.flink.connectors.dummy.source;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordEvaluator;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;

import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.OutputLogEvent;

import javax.annotation.Nullable;

import java.util.Map;

public class CloudWatchLogsSourceReader<T> extends SourceReaderBase<OutputLogEvent, T, CloudWatchLogsSplit, CloudWatchSourceSplitState> {

    public CloudWatchLogsSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<OutputLogEvent>> elementsQueue,
            SplitFetcherManager<OutputLogEvent, CloudWatchLogsSplit> splitFetcherManager,
            RecordEmitter<OutputLogEvent, T, CloudWatchSourceSplitState> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        super(elementsQueue, splitFetcherManager, recordEmitter, config, context);
    }

    public CloudWatchLogsSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<OutputLogEvent>> elementsQueue,
            SplitFetcherManager<OutputLogEvent, CloudWatchLogsSplit> splitFetcherManager,
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
    }

    @Override
    protected void onSplitFinished(Map<String, CloudWatchSourceSplitState> finishedSplitIds) {

    }

    @Override
    protected CloudWatchSourceSplitState initializedState(CloudWatchLogsSplit split) {
        return null;
    }

    @Override
    protected CloudWatchLogsSplit toSplitType(String splitId, CloudWatchSourceSplitState splitState) {
        return null;
    }
}
