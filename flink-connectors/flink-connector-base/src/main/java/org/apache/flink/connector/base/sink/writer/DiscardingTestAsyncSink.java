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

package org.apache.flink.connector.base.sink.writer;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.concurrent.SeparateThreadExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class DiscardingTestAsyncSink<T> extends AsyncSinkBase<T, String> {
    private static final Logger LOG = LoggerFactory.getLogger(DiscardingTestAsyncSink.class);

    public DiscardingTestAsyncSink(long requestTimeoutMS, boolean failOnTimeout) {
        super(
                (element, context) -> element.toString(),
                1,
                1,
                10,
                1000L,
                100,
                500L,
                requestTimeoutMS,
                failOnTimeout);
    }

    @Override
    public SinkWriter<T> createWriter(InitContext context) throws IOException {
        return new DiscardingElementWriter(
                context,
                AsyncSinkWriterConfiguration.builder()
                        .setMaxBatchSize(this.getMaxBatchSize())
                        .setMaxBatchSizeInBytes(this.getMaxBatchSizeInBytes())
                        .setMaxInFlightRequests(this.getMaxInFlightRequests())
                        .setMaxBufferedRequests(this.getMaxBufferedRequests())
                        .setMaxTimeInBufferMS(this.getMaxTimeInBufferMS())
                        .setMaxRecordSizeInBytes(this.getMaxRecordSizeInBytes())
                        .setFailOnTimeout(this.getFailOnTimeout())
                        .setRequestTimeoutMS(this.getRequestTimeoutMS())
                        .build(),
                null);
    }

    @Override
    public SinkWriter<T> createWriter(WriterInitContext context) throws IOException {
        return new DiscardingElementWriter(
                new InitContextWrapper(context),
                AsyncSinkWriterConfiguration.builder()
                        .setMaxBatchSize(this.getMaxBatchSize())
                        .setMaxBatchSizeInBytes(this.getMaxBatchSizeInBytes())
                        .setMaxInFlightRequests(this.getMaxInFlightRequests())
                        .setMaxBufferedRequests(this.getMaxBufferedRequests())
                        .setMaxTimeInBufferMS(this.getMaxTimeInBufferMS())
                        .setMaxRecordSizeInBytes(this.getMaxRecordSizeInBytes())
                        .setFailOnTimeout(this.getFailOnTimeout())
                        .setRequestTimeoutMS(this.getRequestTimeoutMS())
                        .build(),
                Collections.emptyList());
    }

    @Override
    public StatefulSinkWriter<T, BufferedRequestState<String>> restoreWriter(
            InitContext context, Collection<BufferedRequestState<String>> recoveredState)
            throws IOException {
        return super.restoreWriter(context, recoveredState);
    }

    @Override
    public StatefulSinkWriter<T, BufferedRequestState<String>> restoreWriter(
            WriterInitContext context, Collection<BufferedRequestState<String>> recoveredState)
            throws IOException {
        return new DiscardingElementWriter(
                new InitContextWrapper(context),
                AsyncSinkWriterConfiguration.builder()
                        .setMaxBatchSize(this.getMaxBatchSize())
                        .setMaxBatchSizeInBytes(this.getMaxBatchSizeInBytes())
                        .setMaxInFlightRequests(this.getMaxInFlightRequests())
                        .setMaxBufferedRequests(this.getMaxBufferedRequests())
                        .setMaxTimeInBufferMS(this.getMaxTimeInBufferMS())
                        .setMaxRecordSizeInBytes(this.getMaxRecordSizeInBytes())
                        .setFailOnTimeout(this.getFailOnTimeout())
                        .setRequestTimeoutMS(this.getRequestTimeoutMS())
                        .build(),
                recoveredState);
    }

    @Override
    public SimpleVersionedSerializer<BufferedRequestState<String>> getWriterStateSerializer() {
        return new DiscardingElementWriterStateSerializer();
    }

    class DiscardingElementWriter extends AsyncSinkWriter<T, String> {
        SeparateThreadExecutor executor =
                new SeparateThreadExecutor(r -> new Thread(r, "DiscardingElementWriter"));

        public DiscardingElementWriter(
                Sink.InitContext context,
                AsyncSinkWriterConfiguration configuration,
                Collection<BufferedRequestState<String>> bufferedRequestStates) {
            super(
                    (element, context1) -> element.toString(),
                    context,
                    configuration,
                    bufferedRequestStates);
        }

        @Override
        protected long getSizeInBytes(String requestEntry) {
            return requestEntry.length();
        }

        @Override
        protected void submitRequestEntries(
                List<String> requestEntries, ResultHandler<String> resultHandler) {
            executor.execute(
                    () -> {
                        long delayMillis = new Random().nextInt(5000);
                        try {
                            Thread.sleep(delayMillis);
                        } catch (InterruptedException ignored) {
                        }
                        for (String entry : requestEntries) {
                            LOG.info("Discarding {} after {} ms", entry, delayMillis);
                        }

                        resultHandler.complete();
                    });
        }
    }

    static class DiscardingElementWriterStateSerializer
            extends AsyncSinkWriterStateSerializer<String> {
        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        protected void serializeRequestToStream(String request, DataOutputStream out)
                throws IOException {
            out.writeInt(request.length());
            out.write(request.getBytes());
        }

        @Override
        protected String deserializeRequestFromStream(long requestSize, DataInputStream in)
                throws IOException {
            int size = in.readInt();
            byte[] bytes = new byte[size];
            in.readFully(bytes);
            return new String(bytes);
        }
    }
}
