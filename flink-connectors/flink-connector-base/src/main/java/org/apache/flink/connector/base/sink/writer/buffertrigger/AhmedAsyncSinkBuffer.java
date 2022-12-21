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

import org.apache.flink.api.common.operators.ProcessingTimeService;

import java.io.Serializable;

public class AhmedAsyncSinkBuffer<RequestEntryT extends Serializable>
        extends AsyncSinkBuffer<RequestEntryT> {

    protected int bufferSizeInBytes;
    private final int maxBatchSizeInBytes;

    public AhmedAsyncSinkBuffer(
            AsyncSinkBufferFlushTrigger bufferFlushTrigger,
            ProcessingTimeService timeService,
            long maxTimeInBufferMS,
            int maxBufferSize,
            int maxBatchSizeInBytes) {
        super(bufferFlushTrigger, timeService, maxTimeInBufferMS, maxBufferSize);
        this.maxBatchSizeInBytes = maxBatchSizeInBytes;
    }

    @Override
    public boolean add(RequestEntryT entry) {
        bufferSizeInBytes += getRecordSizeInBytes(entry);
        return super.add(entry);
    }

    @Override
    public void addFirst(RequestEntryT entry) {
        bufferSizeInBytes += getRecordSizeInBytes(entry);
        super.addFirst(entry);
    }

    public int countOfEntries() {
        return bufferedRequestEntries.size();
    }

    public RequestEntryT remove() {
        return bufferedRequestEntries.remove();
    }

    public boolean hasAvailableBatch(int batchSize) {
        return super.hasAvailableBatch(batchSize) || bufferSizeInBytes >= maxBatchSizeInBytes;
    }

    protected int getRecordSizeInBytes(RequestEntryT requestEntry) {
        return requestEntry.toString().length();
    }
}
