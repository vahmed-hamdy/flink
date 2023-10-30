package org.apache.flink.connectors.dummy.source.enumerator;

import org.apache.flink.connectors.dummy.source.CloudWatchLogsSplit;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava31.com.google.common.collect.BiMap;
import org.apache.flink.shaded.guava31.com.google.common.collect.HashBiMap;

import org.apache.commons.math3.util.IntegerSequence;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CloudWatchLogsSplitAssigner {
    private List<Set<String>> splitTable;

    private BiMap<Integer, Integer> subtaskIndex = HashBiMap.create();
    private int numberOfReaders;


    public CloudWatchLogsSplitAssigner() {
        this(0);
    }
    public CloudWatchLogsSplitAssigner(int initialNumberOfReaders) {
        splitTable = initializeSplitTable(initialNumberOfReaders, Collections.emptyList());
        numberOfReaders = initialNumberOfReaders;
    }

    private List<Set<String>> initializeSplitTable(int size, List<Set<String>> initial) {
        List<Set<String>> table = new ArrayList<>();
        for(int i=0; i<size; i++) {
            table.add( i < initial.size() ? initial.get(i) : new HashSet<>());
        }
        if(size < initial.size()) {
            initial.subList(size, initial.size()).stream().flatMap(Collection::stream).forEach(split -> assignSplitToTable(split, table));
        }

        return table;
    }

    public int assignSplit(CloudWatchLogsSplit split) {
        return assignSplitToTable(split.splitId(), splitTable);
    }

    private int assignSplitToTable(String split, List<Set<String>> splitTable) {
        AtomicInteger subtask = new AtomicInteger();
        IntegerSequence.range(0, numberOfReaders - 1).forEach((a) -> subtask.set(splitTable.get(a).size() < splitTable.get(
                subtask.get()).size() ? a : subtask.get()));
        splitTable.get(subtask.get()).add(split);
        return subtask.get();
    }

    public void resize(int newNumberOfReaders) {
        splitTable = initializeSplitTable(newNumberOfReaders, splitTable);
        numberOfReaders = newNumberOfReaders;
    }

    public Integer getReaderForSplit(CloudWatchLogsSplit split) {
        return IntStream.range(0, numberOfReaders - 1)
                .filter(i -> splitTable.get(i).contains(split))
                .map(i -> subtaskIndex.inverse().get(i))
                .findFirst()
                .orElseThrow(() -> new FlinkRuntimeException("Fetching non-present split"));

    }
    public void addReader(int reader) {
        resize(numberOfReaders + 1);
        subtaskIndex.put(reader, numberOfReaders - 1);
    }

    public void addSplitsToReader(int reader, List<String> splits) {
        splitTable.get(subtaskIndex.get(reader)).addAll(splits);
    }

    public boolean remove(String splitId) {
        for(Integer subtasks: subtaskIndex.keySet()) {
            if(remove(subtasks, splitId)) {
                return true;
            }
        }
        return false;
    }

    public boolean remove(int subtaskId, String splitId) {
        return splitTable.get(subtaskIndex.get(subtaskId)).remove(splitId);
    }

    public CloudWatchLogsEnumState.AssignedSplitState[] assignedState() {
        List<CloudWatchLogsEnumState.AssignedSplitState> assignedSplitStateList = new ArrayList<>();
        for(Integer reader : subtaskIndex.keySet()) {
            assignedSplitStateList
                    .addAll(splitTable.get(subtaskIndex.get(reader))
                            .stream()
                            .map(s -> new CloudWatchLogsEnumState.AssignedSplitState(reader, s))
                            .collect(Collectors.toList()));
        }
        return assignedSplitStateList.toArray(new CloudWatchLogsEnumState.AssignedSplitState[0]);
    }

    public void getAssignerFromStream(DataInputStream in) throws IOException {
        int numOfReaders = in.readInt();// Ignore
        Integer[] subtasks = new Integer[numOfReaders];
        for (int i = 0; i < numOfReaders; i++) {
            Integer subtask = in.readInt();
            Integer index = in.readInt();
            subtasks[index] = subtask;
        }

        for (int i = 0; i < numOfReaders; i++) {
            addReader(subtasks[i]);
        }
        for (int i = 0; i < numOfReaders; i++) {
            int splitLen = in.readInt();
            List<String> splits = new ArrayList<>();
            while (splitLen > 0) {
                splits.add(in.readUTF());
                splitLen--;
            }
            addSplitsToReader(i, splits);
        }
    }
}
