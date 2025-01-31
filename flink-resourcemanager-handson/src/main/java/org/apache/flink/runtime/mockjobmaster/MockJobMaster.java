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

package org.apache.flink.runtime.mockjobmaster;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.Constants;
import org.apache.flink.runtime.blocklist.BlockedNode;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraphID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.SerializedInputSplit;
import org.apache.flink.runtime.jobmaster.TaskManagerRegistrationInformation;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.TestingResourceManager;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.shuffle.PartitionWithMetrics;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorToJobManagerHeartbeatPayload;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.function.TriConsumer;
import org.apache.flink.util.function.TriFunction;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class MockJobMaster extends TestingJobMasterGateway {
    private final JobID jobId;
    private final JobMasterId jobMasterId;

    private final ResourceID jobMasterResourceId;

    private final TestingResourceManager resourceManager;


    public MockJobMaster(
            @Nonnull String address,
            @Nonnull String hostname,
            @Nonnull Supplier<CompletableFuture<Acknowledge>> cancelFunction,
            @Nonnull Function<TaskExecutionState, CompletableFuture<Acknowledge>> updateTaskExecutionStateFunction,
            @Nonnull BiFunction<JobVertexID, ExecutionAttemptID, CompletableFuture<SerializedInputSplit>> requestNextInputSplitFunction,
            @Nonnull BiFunction<IntermediateDataSetID, ResultPartitionID, CompletableFuture<ExecutionState>> requestPartitionStateFunction,
            @Nonnull Function<ResourceID, CompletableFuture<Acknowledge>> disconnectTaskManagerFunction,
            @Nonnull Consumer<ResourceManagerId> disconnectResourceManagerConsumer,
            @Nonnull BiFunction<ResourceID, Collection<SlotOffer>, CompletableFuture<Collection<SlotOffer>>> offerSlotsFunction,
            @Nonnull TriConsumer<ResourceID, AllocationID, Throwable> failSlotConsumer,
            @Nonnull BiFunction<JobID, TaskManagerRegistrationInformation, CompletableFuture<RegistrationResponse>> registerTaskManagerFunction,
            @Nonnull BiFunction<ResourceID, TaskExecutorToJobManagerHeartbeatPayload, CompletableFuture<Void>> taskManagerHeartbeatFunction,
            @Nonnull Function<ResourceID, CompletableFuture<Void>> resourceManagerHeartbeatFunction,
            @Nonnull Supplier<CompletableFuture<JobStatus>> requestJobStatusSupplier,
            @Nonnull Supplier<CompletableFuture<ExecutionGraphInfo>> requestJobSupplier,
            @Nonnull Supplier<CompletableFuture<CheckpointStatsSnapshot>> checkpointStatsSnapshotSupplier,
            @Nonnull TriFunction<String, Boolean, SavepointFormatType, CompletableFuture<String>> triggerSavepointFunction,
            @Nonnull Function<CheckpointType, CompletableFuture<CompletedCheckpoint>> triggerCheckpointFunction,
            @Nonnull TriFunction<String, Boolean, SavepointFormatType, CompletableFuture<String>> stopWithSavepointFunction,
            @Nonnull Consumer<Tuple5<JobID, ExecutionAttemptID, Long, CheckpointMetrics, TaskStateSnapshot>> acknowledgeCheckpointConsumer,
            @Nonnull Consumer<DeclineCheckpoint> declineCheckpointConsumer,
            @Nonnull Supplier<JobMasterId> fencingTokenSupplier,
            @Nonnull BiFunction<JobID, String, CompletableFuture<KvStateLocation>> requestKvStateLocationFunction,
            @Nonnull Function<Tuple6<JobID, JobVertexID, KeyGroupRange, String, KvStateID, InetSocketAddress>, CompletableFuture<Acknowledge>> notifyKvStateRegisteredFunction,
            @Nonnull Function<Tuple4<JobID, JobVertexID, KeyGroupRange, String>, CompletableFuture<Acknowledge>> notifyKvStateUnregisteredFunction,
            @Nonnull TriFunction<String, Object, byte[], CompletableFuture<Object>> updateAggregateFunction,
            @Nonnull TriFunction<ExecutionAttemptID, OperatorID, SerializedValue<OperatorEvent>, CompletableFuture<Acknowledge>> operatorEventSender,
            @Nonnull BiFunction<OperatorID, SerializedValue<CoordinationRequest>, CompletableFuture<CoordinationResponse>> deliverCoordinationRequestFunction,
            @Nonnull Consumer<Collection<ResourceRequirement>> notifyNotEnoughResourcesConsumer,
            @Nonnull Function<Collection<BlockedNode>, CompletableFuture<Acknowledge>> notifyNewBlockedNodesFunction,
            @Nonnull Supplier<CompletableFuture<JobResourceRequirements>> requestJobResourceRequirementsSupplier,
            @Nonnull Function<JobResourceRequirements, CompletableFuture<Acknowledge>> updateJobResourceRequirementsFunction,
            @Nonnull BiFunction<Duration, Set<ResultPartitionID>, CompletableFuture<Collection<PartitionWithMetrics>>> getPartitionWithMetricsFunction,
            JobID jobId,
            JobMasterId jobMasterId,
            ResourceID jobMasterResourceId,
            TestingResourceManager resourceManager) {
        super(
                address,
                hostname,
                cancelFunction,
                updateTaskExecutionStateFunction,
                requestNextInputSplitFunction,
                requestPartitionStateFunction,
                disconnectTaskManagerFunction,
                disconnectResourceManagerConsumer,
                offerSlotsFunction,
                failSlotConsumer,
                registerTaskManagerFunction,
                taskManagerHeartbeatFunction,
                resourceManagerHeartbeatFunction,
                requestJobStatusSupplier,
                requestJobSupplier,
                checkpointStatsSnapshotSupplier,
                triggerSavepointFunction,
                triggerCheckpointFunction,
                stopWithSavepointFunction,
                acknowledgeCheckpointConsumer,
                declineCheckpointConsumer,
                fencingTokenSupplier,
                requestKvStateLocationFunction,
                notifyKvStateRegisteredFunction,
                notifyKvStateUnregisteredFunction,
                updateAggregateFunction,
                operatorEventSender,
                deliverCoordinationRequestFunction,
                notifyNotEnoughResourcesConsumer,
                notifyNewBlockedNodesFunction,
                requestJobResourceRequirementsSupplier,
                updateJobResourceRequirementsFunction,
                getPartitionWithMetricsFunction);
        this.jobId = jobId;
        this.jobMasterId = jobMasterId;
        this.jobMasterResourceId = jobMasterResourceId;
        this.resourceManager = resourceManager;
    }


    public void start() {
        ((TestingRpcService)resourceManager.getRpcService()).registerGateway(getAddress(), this);
        resourceManager.registerJobMaster(jobMasterId, jobMasterResourceId, getAddress(), jobId, Duration.ofMinutes(3))
                .whenComplete(
                        (registrationResponse, throwable) -> {
                            if (throwable != null) {
                                throw new RuntimeException("Could not register JobMaster at ResourceManager.", throwable);
                            } else {
                                System.out.println("Registered JobMaster at ResourceManager.");
                                System.out.println("JobMaster registration response: " + registrationResponse);
                            }
                        }).join();
        System.out.println("Started JobMaster.");
    }

    public void submitJob(int numSlots) throws IOException {
        SlotReport slotStatuses = resourceManager.getSlotReport();
        System.out.println("Total Slots: " + slotStatuses.getNumSlotStatus());
        if(slotStatuses.getNumSlotStatus() < numSlots){
            throw new RuntimeException("Not enough slots available.");
        }

        List<SlotStatus> statusList = new ArrayList<>();

        slotStatuses.iterator().forEachRemaining(slotStatus -> {
            System.out.println("Slot Status: " + slotStatus);
            statusList.add(slotStatus);
        });

        CompletableFuture<AllocationID> allocationIDCompletableFuture
                = resourceManager.allocateSlotsToJob(jobId, statusList.subList(0, numSlots), Duration.ofMinutes(3));

        AllocationID allocationID = allocationIDCompletableFuture.join();
        resourceManager.declareRequiredResources(jobMasterId,
                ResourceRequirements.create(jobId, getAddress(),
                        Set.of(ResourceRequirement.create(ResourceProfile.ANY, numSlots))),
                Duration.ofMinutes(3)).join();
        System.out.println("declared resources job with " + numSlots + " slots.");
        TaskDeploymentDescriptor tdd = createFakeTask(allocationID);

        resourceManager.getTaskExecutorGateway(allocationID)
                .submitTask(tdd, jobMasterId, Duration.ofMinutes(3)).join();
        System.out.println("Submitted task Horaaay.");

    }

    private TaskDeploymentDescriptor createFakeTask(AllocationID allocationID) throws IOException {
        ExecutionAttemptID executionAttemptID = new ExecutionAttemptID(
                new ExecutionGraphID(),
                new ExecutionVertexID(new JobVertexID(), 0),
                1);

        return new TaskDeploymentDescriptor(
                jobId,
                new TaskDeploymentDescriptor.NonOffloaded<>(new SerializedValue<>(getMockJobInformation())),
                new TaskDeploymentDescriptor.NonOffloaded<>(new SerializedValue<>(getMockTaskInformation(executionAttemptID))),
                executionAttemptID,
                allocationID,
                null,
                Collections.emptyList(),
                Collections.emptyList());
    }


    public JobID getJobId() {
        return jobId;
    }

    public JobMasterId getJobMasterId() {
        return jobMasterId;
    }

    @Override
    public JobMasterId getFencingToken() {
        return jobMasterId;
    }

    private JobInformation getMockJobInformation() throws IOException {
        return new JobInformation(
                jobId,
                JobType.STREAMING,
                "JobName",
                new SerializedValue<ExecutionConfig>(new ExecutionConfig()),
                Constants.getCommonConfiguration(Constants.Role.JOB_MANAGER, false),
                Collections.emptyList(),
                Collections.emptyList());

    }

    private TaskInformation getMockTaskInformation(ExecutionAttemptID attemptID) throws IOException {
        return new TaskInformation(attemptID.getJobVertexId(), "TaskName",
                2,
                2,
                "theClass",
                Constants.getCommonConfiguration(Constants.Role.TASK_MANAGER, false));
    }

    public static MockJobMasterBuilder builder() {
        return new MockJobMasterBuilder();
    }

    public static class MockJobMasterBuilder extends TestingJobMasterGatewayBuilder{

        private JobID jobId;
        private JobMasterId jobMasterId;
        private ResourceID resourceManagerId;

        private TestingResourceManager resourceManager;

        public MockJobMasterBuilder setJobId(JobID jobId){
            this.jobId = jobId;
            return this;
        }

        public MockJobMasterBuilder setJobMasterId(JobMasterId jobMasterId){
            this.jobMasterId = jobMasterId;
            return this;
        }

        public MockJobMasterBuilder setResourceManagerId(ResourceID resourceManagerId){
            this.resourceManagerId = resourceManagerId;
            return this;
        }

        public MockJobMasterBuilder setResourceManager(TestingResourceManager resourceManager){
            this.resourceManager = resourceManager;
            return this;
        }

        @Override
        public MockJobMaster build(){
            return new MockJobMaster(
                address,
                hostname,
                cancelFunction,
                updateTaskExecutionStateFunction,
                requestNextInputSplitFunction,
                requestPartitionStateFunction,
                disconnectTaskManagerFunction,
                disconnectResourceManagerConsumer,
                offerSlotsFunction,
                failSlotConsumer,
                registerTaskManagerFunction,
                taskManagerHeartbeatFunction,
                resourceManagerHeartbeatFunction,
                requestJobStatusSupplier,
                requestJobSupplier,
                checkpointStatsSnapshotSupplier,
                triggerSavepointFunction,
                triggerCheckpointFunction,
                stopWithSavepointFunction,
                acknowledgeCheckpointConsumer,
                declineCheckpointConsumer,
                fencingTokenSupplier,
                requestKvStateLocationFunction,
                notifyKvStateRegisteredFunction,
                notifyKvStateUnregisteredFunction,
                updateAggregateFunction,
                operatorEventSender,
                deliverCoordinationRequestFunction,
                notifyNotEnoughResourcesConsumer,
                notifyNewBlockedNodesFunction,
                requestJobResourceRequirementsSupplier,
                updateJobResourceRequirementsFunction,
                getPartitionWithMetricsFunction,
                    jobId, jobMasterId, resourceManagerId, resourceManager);
        }

    }
}
