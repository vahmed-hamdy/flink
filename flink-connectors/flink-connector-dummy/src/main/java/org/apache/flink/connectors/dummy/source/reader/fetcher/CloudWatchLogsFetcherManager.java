package org.apache.flink.connectors.dummy.source.reader.fetcher;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;

import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connectors.dummy.source.CloudWatchLogsSplit;

import software.amazon.awssdk.services.cloudwatchlogs.model.OutputLogEvent;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class CloudWatchLogsFetcherManager extends SplitFetcherManager<OutputLogEvent, CloudWatchLogsSplit> {

    private final Map<String, SplitFetcher<OutputLogEvent, CloudWatchLogsSplit>> streamFetchers = new HashMap<>();
    public CloudWatchLogsFetcherManager(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<OutputLogEvent>> elementsQueue,
            Supplier<SplitReader<OutputLogEvent, CloudWatchLogsSplit>> splitReaderFactory,
            Configuration configuration) {
        super(elementsQueue, splitReaderFactory, configuration);
    }

    public CloudWatchLogsFetcherManager(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<OutputLogEvent>> elementsQueue,
            Supplier<SplitReader<OutputLogEvent, CloudWatchLogsSplit>> splitReaderFactory,
            Configuration configuration,
            Consumer<Collection<String>> splitFinishedHook) {
        super(elementsQueue, splitReaderFactory, configuration, splitFinishedHook);
    }

    @Override
    public void addSplits(List<CloudWatchLogsSplit> splitsToAdd) {
        splitsToAdd.forEach(split -> {

            boolean isRunning = streamFetchers.containsKey(split.splitId());
            SplitFetcher<OutputLogEvent, CloudWatchLogsSplit> fetcher = this.getOrCreateFetcher(split.splitId());
            fetcher.addSplits(Collections.singletonList(split));
            if(!isRunning) {
                startFetcher(fetcher);
            }
        });
    }

    @Override
    public void removeSplits(List<CloudWatchLogsSplit> splitsToRemove) {
        splitsToRemove.forEach(split -> {
            if(streamFetchers.containsKey(split.splitId())) {
                streamFetchers.get(split.splitId()).removeSplits(Collections.singletonList(split));
                streamFetchers.get(split.splitId()).shutdown();
                this.fetchers.remove(streamFetchers.get(split.splitId()).fetcherId());
                streamFetchers.remove(split.splitId());
            }
        });
    }

    private SplitFetcher<OutputLogEvent, CloudWatchLogsSplit> getOrCreateFetcher(String logStream) {
        return streamFetchers.computeIfAbsent(logStream, ls -> createSplitFetcher());
    }
}
