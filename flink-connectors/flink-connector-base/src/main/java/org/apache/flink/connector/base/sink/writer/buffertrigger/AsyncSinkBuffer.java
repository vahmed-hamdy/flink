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
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;

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
    //    protected List<AsyncSinkBufferMonitor<RequestEntryT>> bufferMonitors;
    protected final AsyncSinkBufferFlushTrigger bufferFlushTrigger;
    private final ProcessingTimeService timeService;
    private final long maxTimeInBufferMS;
    private final int maxBufferSize;
    private boolean existsActiveTimerCallback = false;

    public AsyncSinkBuffer(
            AsyncSinkBufferFlushTrigger bufferFlushTrigger,
            ProcessingTimeService timeService,
            long maxTimeInBufferMS,
            int maxBufferSize) {
        this.bufferFlushTrigger = bufferFlushTrigger;
        this.timeService = timeService;
        this.maxTimeInBufferMS = maxTimeInBufferMS;
        this.maxBufferSize = maxBufferSize;
    }

    public boolean add(RequestEntryT entry) {
        if (bufferedRequestEntries.isEmpty() && !existsActiveTimerCallback) {
            registerCallback();
        }
        return bufferedRequestEntries.add(entry);
    }

    public void addFirst(RequestEntryT entry) {
        if (bufferedRequestEntries.isEmpty() && !existsActiveTimerCallback) {
            registerCallback();
        }
        bufferedRequestEntries.addFirst(entry);
    }

    public int countOfEntries() {
        return bufferedRequestEntries.size();
    }

    public RequestEntryT remove() {
        return bufferedRequestEntries.remove();
    }

    public boolean hasAvailableBatch(int batchSize) {
        return bufferedRequestEntries.size() >= batchSize;
    }

    public boolean canAddRequestEntry(RequestEntryT requestEntryT) throws IllegalArgumentException {
        return bufferedRequestEntries.size() < maxBufferSize;
    }

    public List<RequestEntryT> removeBatch(int batchSize) {
        List<RequestEntryT> nextBatch = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            nextBatch.add(bufferedRequestEntries.remove());
        }
        return nextBatch;
    }

    public BufferedRequestState<RequestEntryT> getBufferState() {
        return new BufferedRequestState<>(this.bufferedRequestEntries);
    }

    //    protected List<AsyncSinkBufferMonitor<RequestEntryT>> getBufferMonitors() {
    //        return bufferMonitors;
    //    }

    private void registerCallback() {
        long triggerId = Instant.now().toEpochMilli();
        ProcessingTimeService.ProcessingTimeCallback ptc =
                instant -> {
                    existsActiveTimerCallback = false;
                    while (!bufferedRequestEntries.isEmpty()) {
                        bufferFlushTrigger.triggerFlush(triggerId);
                    }
                };
        timeService.registerTimer(timeService.getCurrentProcessingTime() + maxTimeInBufferMS, ptc);
        existsActiveTimerCallback = true;
    }
    //
    //    public static class TimeBasedAsyncSinkBufferTrigger<RequestEntryT>
    //            extends AsyncSinkBufferTrigger<RequestEntryT> {
    //
    //        TimeBasedAsyncSinkBufferTrigger(AsyncSinkBufferFlushTrigger action) {
    //            super(action);
    //        }
    //
    //        @Override
    //        void notifyEntry(RequestEntryT entry, Callable<Boolean> triggerAction) {}
    //    }
    private static void testExample() {}
}
