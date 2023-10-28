package org.apache.flink.connectors.dummy.source.enumerator;

import org.apache.flink.connectors.dummy.source.CloudWatchLogsSplit;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava31.com.google.common.collect.BiMap;
import org.apache.flink.shaded.guava31.com.google.common.collect.HashBiMap;

import org.apache.commons.math3.util.IntegerSequence;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class CloudWatchLogsSplitAssigner {
    private List<List<CloudWatchLogsSplit>> splitTable;

    private BiMap<Integer, Integer> subtaskIndex = HashBiMap.create();
    private int numberOfReaders;


    public CloudWatchLogsSplitAssigner(List<Integer> initialReaders) {
        this(initialReaders.size());
        for(int i=0;i<initialReaders.size();i++) {
            subtaskIndex.put(initialReaders.get(i), i);
        }
    }

    public CloudWatchLogsSplitAssigner(int initialNumberOfReaders) {
        splitTable = initializeSplitTable(initialNumberOfReaders, Collections.emptyList());
        numberOfReaders = initialNumberOfReaders;
    }

    private List<List<CloudWatchLogsSplit>> initializeSplitTable(int size, List<List<CloudWatchLogsSplit>> initial) {
        List<List<CloudWatchLogsSplit>> table = new ArrayList<>();
        for(int i=0; i<size; i++) {
            table.add( i < initial.size() ? initial.get(i) : new ArrayList<>());
        }
        if(size < initial.size()) {
            initial.subList(size, initial.size()).stream().flatMap(Collection::stream).forEach(split -> assignSplitToTable(split, table));
        }

        return table;
    }

    public int assignSplit(CloudWatchLogsSplit split) {
        return assignSplitToTable(split, splitTable);
    }

    private int assignSplitToTable(CloudWatchLogsSplit split, List<List<CloudWatchLogsSplit>> splitTable) {
        AtomicInteger subtask = new AtomicInteger();
        IntegerSequence.range(0, numberOfReaders).forEach((a) -> subtask.set(splitTable.get(a).size() < splitTable.get(
                subtask.get()).size() ? a : subtask.get()));
        splitTable.get(subtask.get()).add(split);
        return subtask.get();
    }

    public void resize(int newNumberOfReaders) {
        splitTable = initializeSplitTable(newNumberOfReaders, splitTable);
        numberOfReaders = newNumberOfReaders;
    }

    public Integer getReaderForSplit(CloudWatchLogsSplit split) {
        return IntStream.range(0, numberOfReaders)
                .filter(i -> splitTable.get(i).contains(split))
                .map(i -> subtaskIndex.inverse().get(i))
                .findFirst()
                .orElseThrow(() -> new FlinkRuntimeException("Fetching non-present split"));

    }
    public void addReader(int reader) {
        resize(numberOfReaders + 1);
        subtaskIndex.put(reader, numberOfReaders - 1);
    }

    public void addSplitsToReader(int reader, List<CloudWatchLogsSplit> splits) {
        splitTable.get(subtaskIndex.get(reader)).addAll(splits);
    }
}
