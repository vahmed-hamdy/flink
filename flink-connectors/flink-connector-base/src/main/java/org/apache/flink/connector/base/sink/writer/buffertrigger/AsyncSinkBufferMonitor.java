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

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/** Dummy doc. */
@Internal
public abstract class AsyncSinkBufferMonitor<RequestEntryT> {
    private final AsyncSinkBufferFlushTrigger flushTrigger;
    private final List<RequestEntryT> buffer;

    protected AsyncSinkBufferMonitor(
            AsyncSinkBufferFlushTrigger flushTrigger, List<RequestEntryT> buffer) {
        this.flushTrigger = flushTrigger;
        this.buffer = buffer;
    }

    public AsyncSinkBufferFlushTrigger getFlushTrigger() {
        return flushTrigger;
    }

    public List<RequestEntryT> getBuffer() {
        return Collections.unmodifiableList(buffer);
    }

    public abstract void notifyBufferChange(Long triggerId, RequestEntryT addedEntry)
            throws IOException, InterruptedException;
}
