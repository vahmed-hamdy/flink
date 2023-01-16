/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.base.sink.writer.buffertrigger;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/**
 * This is a dummy java doc.
 *
 * @param <RequestEntryT> important type.
 */
@PublicEvolving
public class AsyncSinkBuffer<RequestEntryT extends Serializable> {
    protected final ArrayDeque<RequestEntryT> bufferedRequestEntries = new ArrayDeque<>();
    protected final List<AsyncSinkBufferBlockingStrategy<RequestEntryT>> bufferMonitors;
    protected final List<AsyncSinkBufferFlushTrigger<RequestEntryT>> bufferFlushTriggers;
    //    private final ProcessingTimeService timeService;
    //    private final long maxTimeInBufferMS;
    //    private final int maxBufferSize;
    //    private boolean existsActiveTimerCallback = false;

    public AsyncSinkBuffer(
            List<AsyncSinkBufferBlockingStrategy<RequestEntryT>> bufferMonitors,
            List<AsyncSinkBufferFlushTrigger<RequestEntryT>> bufferFlushTriggers,
            AsyncSinkBufferFlushAction bufferFlushAction) {
        this.bufferMonitors = bufferMonitors;
        this.bufferFlushTriggers = bufferFlushTriggers;
        //        this.bufferFlushTriggers.forEach(action ->
        // action.registerFlushAction(bufferFlushAction));
        //                this.timeService = timeService;
        //        this.maxTimeInBufferMS = maxTimeInBufferMS;
        //        this.maxBufferSize = maxBufferSize;
    }

    public void add(RequestEntryT entry, boolean atHead) throws InterruptedException {
        if (atHead) {
            bufferedRequestEntries.addFirst(entry);
        } else {
            bufferedRequestEntries.add(entry);
        }
        notifyAllFlushTriggersOnAdd(entry);
    }

    public int countOfEntries() {
        return bufferedRequestEntries.size();
    }

    public boolean hasAvailableBatch(int batchSize) {
        return bufferedRequestEntries.size() >= batchSize;
    }

    public boolean canAddRequestEntry(RequestEntryT requestEntry) throws IllegalArgumentException {
        return this.bufferMonitors.stream()
                .noneMatch(monitor -> monitor.shouldBlock(getBufferState(), requestEntry));
    }

    public List<RequestEntryT> removeBatch(int batchSize) throws IOException, InterruptedException {
        List<RequestEntryT> nextBatch = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            RequestEntryT requestEntry = bufferedRequestEntries.remove();
            notifyAllFlushTriggersOnRemove(requestEntry);
            nextBatch.add(requestEntry);
        }
        return nextBatch;
    }

    public BufferedRequestState<RequestEntryT> getBufferState() {
        return new BufferedRequestState<>(this.bufferedRequestEntries);
    }

    public void notifyAllFlushTriggersOnAdd(RequestEntryT requestEntry)
            throws InterruptedException {
        long triggerId = Instant.now().toEpochMilli();
        for (AsyncSinkBufferFlushTrigger<RequestEntryT> trigger : this.bufferFlushTriggers) {
            trigger.notifyAddRequest(requestEntry, triggerId);
        }
    }

    public void notifyAllFlushTriggersOnRemove(RequestEntryT requestEntry)
            throws IOException, InterruptedException {
        long triggerId = Instant.now().toEpochMilli();
        for (AsyncSinkBufferFlushTrigger<RequestEntryT> trigger : this.bufferFlushTriggers) {
            trigger.notifyRemoveRequest(requestEntry, triggerId);
        }
    }

    private static void testExample() {}
}
