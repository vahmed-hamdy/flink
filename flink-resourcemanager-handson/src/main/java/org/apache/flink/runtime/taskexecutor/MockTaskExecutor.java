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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.management.jmx.JMXService;
import org.apache.flink.runtime.Constants;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.TaskThreadInfoResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.registration.RegisteredRpcConnection;
import org.apache.flink.runtime.registration.RegistrationConnectionListener;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.registration.RetryingRegistrationConfiguration;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.ToImplement;
import org.apache.flink.runtime.resourcemanager.TaskExecutorRegistration;
import org.apache.flink.runtime.rest.messages.LogInfo;
import org.apache.flink.runtime.rest.messages.ProfilingInfo;
import org.apache.flink.runtime.rest.messages.ThreadDumpInfo;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcServiceUtils;
import org.apache.flink.runtime.webmonitor.threadinfo.ThreadInfoSamplesRequest;
import org.apache.flink.types.SerializableOptional;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.concurrent.FutureUtils;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MockTaskExecutor extends RpcEndpoint implements TaskExecutorGateway {

    private final ConcurrentMap<SlotID, SlotStatus> slots = new ConcurrentHashMap<>();

    private final ConcurrentMap<SlotID, AllocationID> allocatedSlots = new ConcurrentHashMap<>();

    private final ConcurrentMap<SlotID, JobID> allocatedSlotsToJob = new ConcurrentHashMap<>();
    private final Map<ExecutionAttemptID, TaskDeploymentDescriptor> runningTasks = new ConcurrentHashMap<>();
    private final Map<ExecutionAttemptID, SlotID> taskSlotMapping = new ConcurrentHashMap<>();

    private final Map<JobID, Set<ResultPartitionID>> releasedPartitions = new ConcurrentHashMap<>();

    private final String resourceManagerAddress;
    private final ResourceID taskExecutorId = ResourceID.generate();

    public MockTaskExecutor(
            RpcService service, String resourceManagerAddress) {
        this(service, resourceManagerAddress, 2);

    }
    public MockTaskExecutor(
            RpcService service, String resourceManagerAddress, int numberOfSlots) {
        super(service, RpcServiceUtils.createRandomName("MockTaskExecutor"));
        this.resourceManagerAddress = resourceManagerAddress;
        initializeSlots(getResourceProfile(1024), numberOfSlots);
    }


    private static ResourceProfile getResourceProfile(int memory, int slots) {
        return ResourceProfile.newBuilder().setCpuCores(1.0 * slots).setTaskHeapMemory(MemorySize.ofMebiBytes(memory * slots)).build();
    }

    private static ResourceProfile getResourceProfile(int memory) {
        return ResourceProfile.newBuilder().setCpuCores(1.0).setTaskHeapMemory(MemorySize.ofMebiBytes(memory)).build();
    }

    private void initializeSlots(ResourceProfile profile, int numberOfSlots) {
        for (int i = 0; i < numberOfSlots; i++) {
            SlotID slotId = new SlotID(taskExecutorId, i);
            slots.put(slotId, new SlotStatus(slotId, profile));
        }
    }

    public boolean awaitResourceManagerReadiness(Duration timeout) {
        boolean available = false;
        while (!available && !timeout.isZero()) {
            try {
                Thread.sleep(1000);
                System.out.println("Waiting for resource manager to be available");
                System.out.println("Resource manager address: " + resourceManagerAddress);
                getResourceManagerGateway();
                System.out.println("resource manager is available");
                available = true;
            } catch (RuntimeException | InterruptedException e) {
                timeout = timeout.minus(Duration.ofSeconds(1));
                System.out.println("resource manager not available");
            }
        }
        return available;
    }



    public final void initialize() {
        start();

        TaskExecutorToResourceManagerConnection resourceManagerConnection = new TaskExecutorToResourceManagerConnection(
                        log,
                        getRpcService(),
                        new RetryingRegistrationConfiguration(60000L, 60000L, 10_000L, 10_000L),
                        resourceManagerAddress,
                        ResourceManagerId.fromUuid(Constants.getSingleResourceManagerId()),
                        getMainThreadExecutor(),
                        new RegistrationListener(),
                new TaskExecutorRegistration(
                        getAddress(),
                        taskExecutorId,
                        getRpcService().getPort(),
                        JMXService.getPort().orElse(-1),
                        HardwareDescription.extractFromSystem(0),
                        TaskExecutorMemoryConfiguration
                                .create(Constants.getCommonConfiguration(Constants.Role.TASK_EXECUTOR, false)),
                        getResourceProfile(1024),
                        getResourceProfile(1024 , slots.size()),
                        getAddress()));
        resourceManagerConnection.start();


    }

    ResourceManagerGateway getResourceManagerGateway(){
        return getRpcService().connect(resourceManagerAddress, ResourceManagerId.fromUuid(Constants.getSingleResourceManagerId()),  ResourceManagerGateway.class).join();
    }


    @Override
    public String getAddress() {
        return super.getAddress();
    }

    @Override
    public String getHostname() {
        return super.getHostname();
    }

    @Override
    public CompletableFuture<Acknowledge> requestSlot(
            SlotID slotId,
            JobID jobId,
            AllocationID allocationId,
            ResourceProfile resourceProfile,
            String targetAddress,
            ResourceManagerId resourceManagerId,
            Duration timeout) {
        allocatedSlots.put(slotId, allocationId);
        allocatedSlotsToJob.put(slotId, jobId);
        slots.put(slotId, new SlotStatus(slotId, resourceProfile, jobId, allocationId));
        System.out.println("Allocated slot " + slotId + " for job " + jobId + " with allocation id " + allocationId);
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public CompletableFuture<Acknowledge> submitTask(
            TaskDeploymentDescriptor tdd,
            JobMasterId jobMasterId,
            Duration timeout) {
        ExecutionAttemptID executionAttemptID = tdd.getExecutionAttemptId();
        System.out.println("Received task submission for execution: " + executionAttemptID + " for job " + tdd.getJobId() +
                " with allocation id " + tdd.getAllocationId());

        runningTasks.put(executionAttemptID, tdd);
        SlotID slotId = findAvailableSlot(tdd.getAllocationId());
        taskSlotMapping.put(executionAttemptID, slotId);
        System.out.println("Task submitted: " + executionAttemptID + " for job " + tdd.getJobId());
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    private SlotID findAvailableSlot(AllocationID allocationID) {
        System.out.println("Finding slot for allocation: " + allocationID + " from " + allocatedSlots);
        return allocatedSlots.entrySet().stream().filter(aId -> allocationID.equals(aId.getValue())).map(
                Map.Entry::getKey).findFirst().orElse(null);
    }
    @Override
    public CompletableFuture<Acknowledge> updatePartitions(
            ExecutionAttemptID executionAttemptID,
            Iterable<PartitionInfo> partitionInfos,
            Duration timeout) {
        System.out.println("Updating partitions for execution: " + executionAttemptID);
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public void releasePartitions(JobID jobId, Set<ResultPartitionID> partitionIds) {
        releasedPartitions.computeIfAbsent(jobId, k -> new HashSet<>()).addAll(partitionIds);
        System.out.println("Released partitions for job: " + jobId + " -> " + partitionIds);
    }

    @Override
    public CompletableFuture<Acknowledge> promotePartitions(
            JobID jobId,
            Set<ResultPartitionID> partitionIds) {
        System.out.println("Promoting partitions for job: " + jobId);
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public CompletableFuture<Acknowledge> releaseClusterPartitions(
            Collection<IntermediateDataSetID> dataSetsToRelease,
            Duration timeout) {
        System.out.println("Releasing cluster partitions: " + dataSetsToRelease);
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public CompletableFuture<Acknowledge> cancelTask(
            ExecutionAttemptID executionAttemptID,
            Duration timeout) {
        if (runningTasks.remove(executionAttemptID) != null) {
            taskSlotMapping.remove(executionAttemptID);
            System.out.println("Canceled task: " + executionAttemptID);
            return CompletableFuture.completedFuture(Acknowledge.get());
        }
        return FutureUtils.completedExceptionally(new IllegalStateException("Task not found: " + executionAttemptID));
    }

    @Override
    public CompletableFuture<Acknowledge> freeSlot(
            AllocationID allocationId,
            Throwable cause,
            Duration timeout) {
        SlotID slotToRemove = allocatedSlots.entrySet().stream()
                .filter(entry -> entry.getValue().equals(allocationId))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No slot found for allocation id " + allocationId));
        allocatedSlotsToJob.entrySet().removeIf(entry -> entry.getKey().equals(slotToRemove));
        taskSlotMapping.entrySet().removeIf(entry -> entry.getValue().equals(slotToRemove));
        System.out.println("Freed slot for allocation: " + allocationId);
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public void freeInactiveSlots(JobID jobId, Duration timeout) {
        Set<SlotID> slotsToFree = allocatedSlotsToJob.entrySet().stream()
                .filter(entry -> entry.getValue().equals(jobId))
                .map(Map.Entry::getKey)
                .collect(HashSet::new, Set::add, Set::addAll);
        allocatedSlots.entrySet().removeIf(entry -> slotsToFree.contains(entry.getKey()));
        System.out.println("Freed inactive slots for job: " + jobId);
    }

    @Override
    public CompletableFuture<Acknowledge> triggerCheckpoint(
            ExecutionAttemptID executionAttemptID,
            long checkpointID,
            long checkpointTimestamp,
            CheckpointOptions checkpointOptions) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public CompletableFuture<Acknowledge> confirmCheckpoint(
            ExecutionAttemptID executionAttemptID,
            long completedCheckpointId,
            long completedCheckpointTimestamp,
            long lastSubsumedCheckpointId) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public CompletableFuture<Acknowledge> abortCheckpoint(
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            long latestCompletedCheckpointId,
            long checkpointTimestamp) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }


    @Override
    public CompletableFuture<Void> heartbeatFromJobManager(
            ResourceID heartbeatOrigin,
            AllocatedSlotReport allocatedSlotReport) {
        System.out.println("Received heartbeat from job manager " + heartbeatOrigin);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> heartbeatFromResourceManager(ResourceID heartbeatOrigin) {
        System.out.println("Received heartbeat from resource manager " + heartbeatOrigin);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void disconnectJobManager(JobID jobId, Exception cause) {
        System.out.println("Disconnected job manager for job " + jobId);
    }

    @Override
    public void disconnectResourceManager(Exception cause) {
        System.out.println("Disconnected resource manager");
    }


    @Override
    public CompletableFuture<TransientBlobKey> requestFileUploadByType(
            FileType fileType,
            Duration timeout) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public CompletableFuture<TransientBlobKey> requestFileUploadByName(
            String fileName,
            Duration timeout) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public CompletableFuture<TransientBlobKey> requestFileUploadByNameAndType(
            String fileName,
            FileType fileType,
            Duration timeout) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public CompletableFuture<SerializableOptional<String>> requestMetricQueryServiceAddress(Duration timeout) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public CompletableFuture<Boolean> canBeReleased() {
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public CompletableFuture<Collection<LogInfo>> requestLogList(Duration timeout) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public CompletableFuture<Acknowledge> sendOperatorEventToTask(
            ExecutionAttemptID task,
            OperatorID operator,
            SerializedValue<OperatorEvent> evt) {
        System.out.println("Received operator event " + evt + " from " + operator + " for task " + task);
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public CompletableFuture<ThreadDumpInfo> requestThreadDump(Duration timeout) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public CompletableFuture<Acknowledge> updateDelegationTokens(
            ResourceManagerId resourceManagerId,
            byte[] tokens) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    @ToImplement
    public CompletableFuture<ProfilingInfo> requestProfiling(
            int duration,
            ProfilingInfo.ProfilingMode mode,
            Duration timeout) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    @ToImplement
    public CompletableFuture<Collection<ProfilingInfo>> requestProfilingList(Duration timeout) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    @ToImplement
    public CompletableFuture<TaskThreadInfoResponse> requestThreadInfoSamples(
            Collection<ExecutionAttemptID> taskExecutionAttemptIds,
            ThreadInfoSamplesRequest requestParams,
            Duration timeout) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    private class RegistrationListener implements RegistrationConnectionListener<TaskExecutorToResourceManagerConnection, TaskExecutorRegistrationSuccess, TaskExecutorRegistrationRejection> {

        @Override
        public void onRegistrationSuccess(TaskExecutorToResourceManagerConnection connection, TaskExecutorRegistrationSuccess success) {
            System.out.println("TaskExecutor registered successfully " + connection.getTargetAddress());
            System.out.println("TaskExecutor ID: " + taskExecutorId);
            getResourceManagerGateway().sendSlotReport(
                    taskExecutorId,
                    success.getRegistrationId(),
                    createSlotReport(),
                    Duration.ofMinutes(3)
            ).whenComplete(
                    (ignored, throwable) -> {
                        if (throwable != null) {
                            System.out.println("Failed to send slot report to resource manager: " + throwable.getMessage());
                        } else {
                            System.out.println("Slot report sent to resource manager");
                        }
                    }
            ).join();

        }

        @Override
        public void onRegistrationFailure(Throwable failure) {
            System.out.println("TaskExecutor registration failed: " + failure.getMessage());
        }

        @Override
        public void onRegistrationRejection(
                String targetAddress,
                TaskExecutorRegistrationRejection rejection) {
            System.out.println("TaskExecutor registration rejected: " + rejection);
        }

    }

    private SlotReport createSlotReport() {
        System.out.println("Creating slot report with " + slots.size() + " slots");
        return new SlotReport(slots.values());
    }
}
