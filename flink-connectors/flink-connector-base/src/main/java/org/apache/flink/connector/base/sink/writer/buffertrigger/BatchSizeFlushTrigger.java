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

public class BatchSizeFlushTrigger<RequestEntryT>
        implements AsyncSinkBufferFlushTrigger<RequestEntryT> {

    private final List<AsyncSinkBufferFlushAction> flushTriggers = new ArrayList<>();
    private final int maxBatchSize;
    private int currentBatchSize;

    public BatchSizeFlushTrigger(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
        this.currentBatchSize = 0;
    }

    @Override
    public void registerFlushAction(AsyncSinkBufferFlushAction flushAction) {
        flushTriggers.add(flushAction);
    }

    @Override
    public void notifyAddRequest(RequestEntryT requestAdded, long triggerId)
            throws InterruptedException {
        currentBatchSize++;
        if (currentBatchSize >= maxBatchSize) {
            for (AsyncSinkBufferFlushAction triggerAction : this.flushTriggers) {
                triggerAction.triggerFlush(triggerId);
            }
        }
    }

    @Override
    public void notifyRemoveRequest(RequestEntryT requestRemoved, long triggerId)
            throws InterruptedException {
        currentBatchSize--;
    }

    @Override
    public boolean willTriggerOnAdd(RequestEntryT requestAdded) {
        return (currentBatchSize + 1 >= maxBatchSize);
    }
}
