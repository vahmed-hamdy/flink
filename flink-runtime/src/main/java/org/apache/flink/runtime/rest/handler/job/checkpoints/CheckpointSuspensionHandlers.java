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
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.async.AbstractAsynchronousOperationHandlers;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationInfo;
import org.apache.flink.runtime.rest.handler.job.AsynchronousJobOperationKey;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.runtime.rest.messages.TriggerIdPathParameter;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointSuspensionStatusHeaders;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointSuspensionStatusMessageParameters;
import org.apache.flink.runtime.rest.messages.job.checkpoints.CheckpointSuspensionActionHeaders;
import org.apache.flink.runtime.rest.messages.job.checkpoints.CheckpointSuspensionRequest;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.SerializedThrowable;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

/** Handlers to trigger the suspensions of a checkpoint coordinater. */
public class CheckpointSuspensionHandlers
        extends AbstractAsynchronousOperationHandlers<AsynchronousJobOperationKey, Acknowledge> {
    public class CheckpointSuspensionTriggerHandler
            extends TriggerHandler<
                    RestfulGateway, CheckpointSuspensionRequest, JobMessageParameters> {

        protected CheckpointSuspensionTriggerHandler(
                GatewayRetriever<? extends RestfulGateway> leaderRetriever,
                Time timeout,
                Map<String, String> responseHeaders) {
            super(
                    leaderRetriever,
                    timeout,
                    responseHeaders,
                    CheckpointSuspensionActionHeaders.getInstance());
        }

        @Override
        protected CompletableFuture<Acknowledge> triggerOperation(
                HandlerRequest<CheckpointSuspensionRequest, JobMessageParameters> request,
                RestfulGateway gateway)
                throws RestHandlerException {
            final CheckpointSuspensionRequest.CheckpointSuspensionAction checkpointingAction =
                    request.getRequestBody().getAction();
            final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
            final CompletableFuture<Acknowledge> checkpointingActionFuture;
            switch (checkpointingAction) {
                case SUSPEND:
                    return gateway.suspendCheckpointing(jobId, RpcUtils.INF_TIMEOUT);
                case RESUME:
                    return gateway.resumeCheckpointing(jobId, RpcUtils.INF_TIMEOUT);
                default:
                    checkpointingActionFuture =
                            FutureUtils.completedExceptionally(
                                    new RestHandlerException(
                                            "Unknown checkpointing action "
                                                    + checkpointingAction
                                                    + '.',
                                            HttpResponseStatus.BAD_REQUEST));
            }

            return checkpointingActionFuture.handle(
                    (Acknowledge ack, Throwable throwable) -> {
                        if (throwable != null) {
                            Throwable error = ExceptionUtils.stripCompletionException(throwable);

                            if (error instanceof TimeoutException) {
                                throw new CompletionException(
                                        new RestHandlerException(
                                                "Checkpointing action timed out.",
                                                HttpResponseStatus.REQUEST_TIMEOUT,
                                                error));
                            } else if (error instanceof FlinkJobNotFoundException) {
                                throw new CompletionException(
                                        new RestHandlerException(
                                                "Job could not be found.",
                                                HttpResponseStatus.NOT_FOUND,
                                                error));
                            } else {
                                throw new CompletionException(
                                        new RestHandlerException(
                                                "Checkpointing action failed: "
                                                        + error.getMessage(),
                                                HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                                error));
                            }
                        } else {
                            return Acknowledge.get();
                        }
                    });
        }

        @Override
        protected AsynchronousJobOperationKey createOperationKey(
                HandlerRequest<CheckpointSuspensionRequest, JobMessageParameters> request) {
            final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
            return AsynchronousJobOperationKey.of(new TriggerId(), jobId);
        }
    }

    /** {@link StatusHandler} implementation for the savepoint disposal operation. */
    public class CheckpointSuspensionStatusHandler
            extends StatusHandler<
                    RestfulGateway,
                    AsynchronousOperationInfo,
                    CheckpointSuspensionStatusMessageParameters> {

        public CheckpointSuspensionStatusHandler(
                GatewayRetriever<? extends RestfulGateway> leaderRetriever,
                Time timeout,
                Map<String, String> responseHeaders) {
            super(
                    leaderRetriever,
                    timeout,
                    responseHeaders,
                    CheckpointSuspensionStatusHeaders.getInstance());
        }

        @Override
        protected AsynchronousJobOperationKey getOperationKey(
                HandlerRequest<EmptyRequestBody, CheckpointSuspensionStatusMessageParameters>
                        request) {
            final TriggerId triggerId = request.getPathParameter(TriggerIdPathParameter.class);
            final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
            return AsynchronousJobOperationKey.of(triggerId, jobId);
        }

        @Override
        protected AsynchronousOperationInfo exceptionalOperationResultResponse(
                Throwable throwable) {
            return AsynchronousOperationInfo.completeExceptional(
                    new SerializedThrowable(throwable));
        }

        @Override
        protected AsynchronousOperationInfo operationResultResponse(Acknowledge operationResult) {
            return AsynchronousOperationInfo.complete();
        }
    }
}
