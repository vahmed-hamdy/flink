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

package org.apache.flink.runtime.rest.handler.job.checkpoints;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.job.checkpoints.CheckpointSuspensionRequest;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * This is a test for {@link
 * org.apache.flink.runtime.rest.handler.job.checkpoints.CheckpointSuspensionHandlers}
 */
public class CheckpointSuspensionHandlersTest extends TestLogger {

    private static final Time TIMEOUT = Time.seconds(10);

    private static final JobID JOB_ID = new JobID();

    private GatewayRetriever<RestfulGateway> leaderRetriever;

    private CheckpointSuspensionHandlers.CheckpointSuspensionTriggerHandler
            checkpointSuspensionTriggerHandler;

    private CheckpointSuspensionHandlers.CheckpointSuspensionStatusHandler
            checkpointSuspensionStatusHandler;

    @Before
    public void setUp() throws Exception {
        leaderRetriever = () -> CompletableFuture.completedFuture(null);
        CheckpointSuspensionHandlers checkpointSuspensionHandlers =
                new CheckpointSuspensionHandlers();
        checkpointSuspensionTriggerHandler =
                checkpointSuspensionHandlers
                .new CheckpointSuspensionTriggerHandler(
                        leaderRetriever, TIMEOUT, Collections.emptyMap());

        checkpointSuspensionStatusHandler =
                checkpointSuspensionHandlers
                .new CheckpointSuspensionStatusHandler(
                        leaderRetriever, TIMEOUT, Collections.emptyMap());
    }

    @Test
    public void testSuspendCheckpointingCompletes()
            throws HandlerRequestException, RestHandlerException {
        final CompletableFuture<Acknowledge> suspendCheckpointing = new CompletableFuture<>();
        AtomicBoolean suspendCheckpointingCalled = new AtomicBoolean(false);
        final TestingRestfulGateway testingRestfulGateway =
                new TestingRestfulGateway.Builder()
                        .setTriggerSuspendCheckpointing(
                                (JobID jobId) -> {
                                    suspendCheckpointingCalled.set(true);
                                    return suspendCheckpointing;
                                })
                        .build();
        final CheckpointSuspensionHandlers checkpointSuspensionHandlers =
                new CheckpointSuspensionHandlers();
        final CheckpointSuspensionHandlers.CheckpointSuspensionTriggerHandler
                checkpointSuspensionTriggerHandler =
                        checkpointSuspensionHandlers
                        .new CheckpointSuspensionTriggerHandler(
                                leaderRetriever, TIMEOUT, Collections.emptyMap());

        checkpointSuspensionTriggerHandler.handleRequest(
                triggerCheckpointSuspensionRequest(
                        CheckpointSuspensionRequest.CheckpointSuspensionAction.SUSPEND),
                testingRestfulGateway);

        assertThat(suspendCheckpointingCalled.get(), equalTo(true));
    }

    @Test
    public void testResumeCheckpointingCompletes()
            throws HandlerRequestException, RestHandlerException {
        final CompletableFuture<Acknowledge> resumeCheckpointing = new CompletableFuture<>();
        AtomicBoolean resumeCheckpointCalled = new AtomicBoolean(false);
        final TestingRestfulGateway testingRestfulGateway =
                new TestingRestfulGateway.Builder()
                        .setTriggerResumeCheckpointing(
                                (JobID jobId) -> {
                                    resumeCheckpointCalled.set(true);
                                    return resumeCheckpointing;
                                })
                        .build();
        final CheckpointSuspensionHandlers checkpointSuspensionHandlers =
                new CheckpointSuspensionHandlers();
        final CheckpointSuspensionHandlers.CheckpointSuspensionTriggerHandler
                checkpointSuspensionTriggerHandler =
                        checkpointSuspensionHandlers
                        .new CheckpointSuspensionTriggerHandler(
                                leaderRetriever, TIMEOUT, Collections.emptyMap());

        checkpointSuspensionTriggerHandler.handleRequest(
                triggerCheckpointSuspensionRequest(
                        CheckpointSuspensionRequest.CheckpointSuspensionAction.RESUME),
                testingRestfulGateway);

        assertThat(resumeCheckpointCalled.get(), equalTo(true));
    }

    private static HandlerRequest<CheckpointSuspensionRequest, JobMessageParameters>
            triggerCheckpointSuspensionRequest(
                    CheckpointSuspensionRequest.CheckpointSuspensionAction action)
                    throws HandlerRequestException {
        return new HandlerRequest<>(
                new CheckpointSuspensionRequest(action),
                new JobMessageParameters(),
                Collections.singletonMap(JobIDPathParameter.KEY, JOB_ID.toString()),
                Collections.emptyMap());
    }
}
