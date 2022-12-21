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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.ProcessingTimeService;

import java.io.IOException;
import java.util.List;

@Internal
public final class TimeBasedBufferMonitor<RequestEntryT>
        extends AsyncSinkBufferMonitor<RequestEntryT> {

    private final ProcessingTimeService timeService;
    private final long maxTimeInBufferMS;

    private boolean existsActiveTimerCallback = false;

    private TimeBasedBufferMonitor(
            ProcessingTimeService timeService,
            Long maxTimeInBufferMS,
            AsyncSinkBufferFlushTrigger flushTrigger,
            List<RequestEntryT> buffer) {
        super(flushTrigger, buffer);
        this.maxTimeInBufferMS = maxTimeInBufferMS;
        this.timeService = timeService;
    }

    //    @Override
    public void notifyBufferChange(Long triggerId) {
        if (existsActiveTimerCallback || this.getBuffer().size() != 1) {
            return;
        }

        ProcessingTimeService.ProcessingTimeCallback ptc =
                instant -> {
                    existsActiveTimerCallback = false;
                    getFlushTrigger().triggerFlush(triggerId);
                };
        timeService.registerTimer(timeService.getCurrentProcessingTime() + maxTimeInBufferMS, ptc);
        existsActiveTimerCallback = true;
    }

    @Override
    public void notifyBufferChange(Long triggerId, RequestEntryT addedEntry)
            throws IOException, InterruptedException {}
}
