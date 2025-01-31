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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.Constants;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class TestingResourceManager extends StandaloneResourceManager {

    private TaskExecutorGateway taskExecutorGateway;

    private SlotReport slotReport;

    private JobMasterId registeredJobMasterId;

    private JobID registeredJobId;

    private ResourceID registeredJobManagerResourceId;

    private String registeredJobManagerAddress;

    private Map<ResourceID, TaskExecutorGateway> taskExecutorGatewayMap = new ConcurrentHashMap<>();

    private Map<SlotStatus, ResourceID> slotResourceIDMap = new ConcurrentHashMap<>();

    private Map<AllocationID, ResourceID> allocationIDResourceIDMap = new ConcurrentHashMap<>();



    public TestingResourceManager(
            StandaloneResourceManager delegate) {
        super(
                delegate.getRpcService(),
                Constants.getSingleResourceManagerId(),
                delegate.resourceId,
                delegate.heartbeatServices,
                delegate.delegationTokenManager,
                delegate.slotManager,
                taskExecutorClusterPartitionReleaser -> delegate.clusterPartitionTracker,
                (blocklistContext, taskManagerNodeIdRetriever, mainThreadExecutor, log) -> delegate.blocklistHandler,
                delegate.jobLeaderIdService,
                delegate.clusterInformation,
                delegate.fatalErrorHandler,
                delegate.resourceManagerMetricGroup,
                delegate.startupPeriodTime,
                Duration.ofMinutes(5),
                delegate.ioExecutor);
    }

    public void startResourceManger() {
        System.out.println("Starting Test Resource Manager");
        start();
        System.out.println("Started Test Resource Manager with address: " + getAddress());
    }

    @Override
    public CompletableFuture<RegistrationResponse> registerJobMaster(
            final JobMasterId jobMasterId,
            final ResourceID jobManagerResourceId,
            final String jobManagerAddress,
            final JobID jobId,
            final Duration timeout) {
        System.out.println("Registering Job Master with JobID: " + jobId);
        registeredJobMasterId = jobMasterId;
        registeredJobId = jobId;
        registeredJobManagerResourceId = jobManagerResourceId;
        registeredJobManagerAddress = jobManagerAddress;
        return super.registerJobMaster(jobMasterId, jobManagerResourceId, jobManagerAddress, jobId, timeout);
    }


    @Override
    public CompletableFuture<RegistrationResponse> registerTaskExecutor(
            final TaskExecutorRegistration taskExecutorRegistration, final Duration timeout) {
        System.out.println("Registering Task Executor");
        taskExecutorGateway = getRpcService().connect(taskExecutorRegistration.getTaskExecutorAddress(),
                                TaskExecutorGateway.class).join();
        taskExecutorGatewayMap.put(taskExecutorRegistration.getResourceId(), taskExecutorGateway);
        System.out.println("Registered task executor TaskExecutorGateway: " + taskExecutorRegistration.getTaskExecutorAddress());
        return super.registerTaskExecutor(taskExecutorRegistration, timeout);
    }

    public TaskExecutorGateway getTaskExecutorGateway() {
        return taskExecutorGateway;
    }

    public TaskExecutorGateway getTaskExecutorGateway(ResourceID resourceId) {
        return taskExecutorGatewayMap.get(resourceId);
    }

    public TaskExecutorGateway getTaskExecutorGateway(AllocationID allocationID) {
        return taskExecutorGatewayMap.get(allocationIDResourceIDMap.get(allocationID));
    }

    @Override
    public CompletableFuture<Acknowledge> sendSlotReport(
            ResourceID taskManagerResourceId,
            InstanceID taskManagerRegistrationId,
            SlotReport slotReport,
            Duration timeout) {
        System.out.println("Receiving Slot Report on resource manager");
        System.out.println("Slot Report: " + slotReport.getNumSlotStatus());
        slotReport.spliterator().forEachRemaining(slotStatus -> slotResourceIDMap.put(slotStatus, taskManagerResourceId));

        this.slotReport = slotReport;
        return super.sendSlotReport(taskManagerResourceId, taskManagerRegistrationId, slotReport, timeout);
    }

    public SlotReport getSlotReport() {
        return slotReport == null ? new SlotReport() : slotReport;
    }

    public CompletableFuture<AllocationID> allocateSlotsToJob(
            JobID jobId,
            List<SlotStatus> slotStatuses,
            Duration timeout) {
        if(slotStatuses.isEmpty()) {
            return CompletableFuture.completedFuture(new AllocationID());
        }
        ResourceID resourceId = slotResourceIDMap.get(slotStatuses.get(0));

        AllocationID allocationId = new AllocationID();
        slotStatuses.forEach(slotId -> getTaskExecutorGateway(resourceId).requestSlot(
                slotId.getSlotID(),
                jobId,
                allocationId,
                slotId.getResourceProfile(),
                registeredJobManagerAddress,
                getFencingToken(),
                timeout).thenAccept(
                (response) -> {
                    if (response != null) {
                        System.out.println("Slot allocated to job: " + jobId);
                        allocationIDResourceIDMap.put(allocationId, resourceId);
                    }
                }).join());
        return CompletableFuture.completedFuture(allocationId);
    }
}
