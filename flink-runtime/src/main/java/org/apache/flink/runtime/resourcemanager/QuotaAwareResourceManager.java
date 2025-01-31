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

import org.apache.flink.runtime.blocklist.BlocklistHandler;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.io.network.partition.ResourceManagerPartitionTrackerFactory;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.quotamanager.QuotaManagerGateway;
import org.apache.flink.runtime.quotamanager.ResourceQuota;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.slotmanager.NonSupportedResourceAllocatorImpl;
import org.apache.flink.runtime.resourcemanager.slotmanager.ResourceAllocator;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.security.token.DelegationTokenManager;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.util.concurrent.FutureUtils;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

public class QuotaAwareResourceManager extends ResourceManager<ResourceID> {
    private String quotaServerAddress;
    private QuotaManagerGateway quotaServerGateway;
    public QuotaAwareResourceManager(
            RpcService rpcService,
            UUID leaderSessionId,
            ResourceID resourceId,
            HeartbeatServices heartbeatServices,
            DelegationTokenManager delegationTokenManager,
            SlotManager slotManager,
            ResourceManagerPartitionTrackerFactory clusterPartitionTrackerFactory,
            BlocklistHandler.Factory blocklistHandlerFactory,
            JobLeaderIdService jobLeaderIdService,
            ClusterInformation clusterInformation,
            FatalErrorHandler fatalErrorHandler,
            ResourceManagerMetricGroup resourceManagerMetricGroup,
            Duration rpcTimeout,
            Executor ioExecutor) {
        super(
                rpcService,
                leaderSessionId,
                resourceId,
                heartbeatServices,
                delegationTokenManager,
                slotManager,
                clusterPartitionTrackerFactory,
                blocklistHandlerFactory,
                jobLeaderIdService,
                clusterInformation,
                fatalErrorHandler,
                resourceManagerMetricGroup,
                rpcTimeout,
                ioExecutor);
    }

    @Override
    public void close() throws Exception {
        // TODO: delegate to {@link QuotaServer#close()}
        super.close();
    }

    @Override
    public CompletableFuture<Acknowledge> declareRequiredResources(
            JobMasterId jobMasterId, ResourceRequirements resourceRequirements, Duration timeout) {

        int numberOfRequiredSlots = resourceRequirements.getResourceRequirements().stream()
                .mapToInt(org.apache.flink.runtime.slots.ResourceRequirement::getNumberOfRequiredSlots)
                .sum();

        return this.quotaServerGateway.getUserForJob(jobMasterId).thenApply(
                (quotaUserId) -> {
                    if (quotaUserId.isPresent()) {
                        return quotaUserId.get();
                    } else {
                        throw new RuntimeException("Quota user not found for job " + jobMasterId);
                    }
                })
                .thenCompose(
                        (quotaUserId) -> this.quotaServerGateway
                                .allocateAvailableSlots(quotaUserId, numberOfRequiredSlots))
                .thenCompose(
                        (acknowledge) -> {
                            if (acknowledge != Acknowledge.get()) {
                                throw new RuntimeException("Failed to allocate slots for job " + jobMasterId);
                            }

                            return super.declareRequiredResources(jobMasterId, resourceRequirements, timeout);
                        });
    }

    int mapRequirementsWithQuota(int numberOfSlots, ResourceQuota quota) {
        return Math.min(numberOfSlots, quota.getNumSlots());
    }

    @Override
    protected void initialize() throws ResourceManagerException {
        CompletableFuture<QuotaManagerGateway> quotaServerConnection =
                getRpcService().connect(quotaServerAddress, QuotaManagerGateway.class);

        try {
            this.quotaServerGateway = quotaServerConnection.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    protected void terminate() throws Exception {
        // noop
    }

    @Override
    protected void internalDeregisterApplication(
            ApplicationStatus finalStatus,
            @Nullable String optionalDiagnostics) throws ResourceManagerException {
        // TODO: delegate to {@link QuotaServer#internalDeregisterApplication()}
    }

    @Override
    protected Optional<ResourceID> getWorkerNodeIfAcceptRegistration(ResourceID resourceID) {
        return  Optional.of(resourceID);
    }

    @Override
    protected CompletableFuture<Void> getReadyToServeFuture() {
        // TODO: delegate to {@link QuotaServer#getReadyToServeFuture()}
        return CompletableFuture.completedFuture(null);
    }

    @Override
    protected ResourceAllocator getResourceAllocator() {
        return NonSupportedResourceAllocatorImpl.INSTANCE;
    }
}
