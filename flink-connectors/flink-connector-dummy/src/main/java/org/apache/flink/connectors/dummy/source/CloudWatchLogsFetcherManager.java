package org.apache.flink.connectors.dummy.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;

import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.OutputLogEvent;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class CloudWatchLogsFetcherManager extends SingleThreadFetcherManager<OutputLogEvent, CloudWatchLogsSplit> {
    public CloudWatchLogsFetcherManager(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<OutputLogEvent>> elementsQueue,
            Supplier<SplitReader<OutputLogEvent, CloudWatchLogsSplit>> splitReaderSupplier,
            Configuration configuration) {
        super(elementsQueue, splitReaderSupplier, configuration);
    }

    public CloudWatchLogsFetcherManager(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<OutputLogEvent>> elementsQueue,
            Supplier<SplitReader<OutputLogEvent, CloudWatchLogsSplit>> splitReaderSupplier,
            Configuration configuration,
            Consumer<Collection<String>> splitFinishedHook) {
        super(elementsQueue, splitReaderSupplier, configuration, splitFinishedHook);
    }
}
