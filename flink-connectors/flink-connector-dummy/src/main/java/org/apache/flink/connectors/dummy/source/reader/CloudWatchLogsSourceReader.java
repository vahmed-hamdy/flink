package org.apache.flink.connectors.dummy.source.reader;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordEvaluator;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import org.apache.flink.connectors.dummy.source.CloudWatchLogsSplit;
import org.apache.flink.connectors.dummy.source.CloudWatchSourceSplitState;

import org.apache.flink.connectors.dummy.source.FinishedStreamEvent;

import software.amazon.awssdk.services.cloudwatchlogs.model.OutputLogEvent;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;

public class CloudWatchLogsSourceReader<T> extends SourceReaderBase<OutputLogEvent, T, CloudWatchLogsSplit, CloudWatchSourceSplitState> {

    private final SourceReaderContext context;

    public CloudWatchLogsSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<OutputLogEvent>> elementsQueue,
            SplitFetcherManager<OutputLogEvent, CloudWatchLogsSplit> splitFetcherManager,
            RecordEmitter<OutputLogEvent, T, CloudWatchSourceSplitState> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        super(elementsQueue, splitFetcherManager, recordEmitter, config, context);
        this.context = context;
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
        this.context = context;
    }

    @Override
    protected void onSplitFinished(Map<String, CloudWatchSourceSplitState> finishedSplitIds) {
        splitFetcherManager.removeSplits(finishedSplitIds.values().stream().map(
                CloudWatchSourceSplitState::tpSplit).collect(
                Collectors.toList()));

        context.sendSourceEventToCoordinator(new FinishedStreamEvent(new ArrayList<>(finishedSplitIds.keySet())));
    }

    @Override
    protected CloudWatchSourceSplitState initializedState(CloudWatchLogsSplit split) {
        return new CloudWatchSourceSplitState(split);
    }

    @Override
    protected CloudWatchLogsSplit toSplitType(
            String splitId,
            CloudWatchSourceSplitState splitState) {
        return splitState.tpSplit();
    }
}
