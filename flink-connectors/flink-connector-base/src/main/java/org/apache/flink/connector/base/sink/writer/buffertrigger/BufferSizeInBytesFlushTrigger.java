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

import java.util.ArrayList;
import java.util.List;

public abstract class BufferSizeInBytesFlushTrigger<RequestEntryT>
        implements AsyncSinkBufferFlushTrigger<RequestEntryT> {

    private final List<AsyncSinkBufferFlushAction> flushTriggers = new ArrayList<>();
    private final long maxBatchSizeInBytes;
    private long bufferSizeInBytes;

    protected BufferSizeInBytesFlushTrigger(long maxBatchSizeInBytes) {
        this.maxBatchSizeInBytes = maxBatchSizeInBytes;
        this.bufferSizeInBytes = 0L;
    }

    protected abstract int getSizeOfEntry(RequestEntryT addedEntry);

    @Override
    public void registerFlushAction(AsyncSinkBufferFlushAction flushAction) {
        this.flushTriggers.add(flushAction);
    }

    @Override
    public void notifyAddRequest(RequestEntryT requestAdded, long triggerId)
            throws InterruptedException {
        bufferSizeInBytes += getSizeOfEntry(requestAdded);
        if (bufferSizeInBytes >= maxBatchSizeInBytes) {
            for (AsyncSinkBufferFlushAction triggerAction : this.flushTriggers) {
                triggerAction.triggerFlush(triggerId);
            }
        }
    }

    @Override
    public void notifyRemoveRequest(RequestEntryT requestRemoved, long triggerId)
            throws InterruptedException {
        bufferSizeInBytes -= getSizeOfEntry(requestRemoved);
    }

    @Override
    public boolean willTriggerOnAdd(RequestEntryT requestAdded) {
        return (bufferSizeInBytes + getSizeOfEntry(requestAdded) >= maxBatchSizeInBytes);
    }
}
