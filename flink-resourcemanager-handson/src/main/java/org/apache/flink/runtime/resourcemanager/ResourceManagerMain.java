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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.AbstractMain;
import org.apache.flink.runtime.Constants;
import org.apache.flink.runtime.EnvVar;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.NoOpHeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneHaServices;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.SlotManagerMetricGroup;
import org.apache.flink.runtime.mockjobmaster.MockJobMaster;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.security.token.DelegationTokenManager;
import org.apache.flink.runtime.security.token.NoOpDelegationTokenManager;
import org.apache.flink.util.concurrent.Executors;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class ResourceManagerMain implements AbstractMain {

    private EnvVar port = new EnvVar("RESOURCE_MANAGER_PORT", "6123");
    private EnvVar resourceManagerAddress = new EnvVar("RESOURCE_MANAGER_URL", "localhost");

    private TestingResourceManager resourceManager;

    public ResourceManagerMain() {
    }

    @Override
    public void start() throws Exception {
        Configuration config = Constants.getCommonConfiguration(Constants.Role.RESOURCE_MANAGER, false);
        RpcSystem rpcSystem = RpcSystem.load(config);
        JobID jobId = JobID.generate();
        JobMasterId jobMasterId = JobMasterId.generate();
        ResourceID resourceManagerId = ResourceID.generate();


        // Initialize RPC Service
        TestingRpcService rpcService = new TestingRpcService(RpcUtils.createRemoteRpcService(
                rpcSystem,
                config,
                resourceManagerAddress.getValue(),
                String.valueOf(port.getValue()),
                "0.0.0.0",
                Optional.of(Integer.valueOf(port.getValue()))));

        ResourceID resourceId = ResourceID.generate();
        System.out.println("Creating ResourceManager with host: " + resourceManagerAddress.getValue() + " and port: " + port.getValue());

        DelegationTokenManager delegationTokenManager = new NoOpDelegationTokenManager();
        HeartbeatServices heartbeatServices = NoOpHeartbeatServices.getInstance();
        HighAvailabilityServices haServices = new StandaloneHaServices(rpcService.getAddress(), rpcService.getAddress(), rpcService.getAddress());
        ResourceManagerRuntimeServices resourceManagerRuntimeServices =
                ResourceManagerRuntimeServices.fromConfiguration(
                        ResourceManagerRuntimeServicesConfiguration
                                .fromConfiguration(config, ArbitraryWorkerResourceSpecFactory.INSTANCE),
                        haServices,
                       rpcService.getScheduledExecutor(),
                        createSlotManagerMetricGroup(config));
        MockResourceManagerRuntimeServices mockResourceManagerRuntimeServices = new MockResourceManagerRuntimeServices(
                resourceManagerRuntimeServices,
                jobId,
                jobMasterId);

        // Initialize ResourceManager


         resourceManager = new TestingResourceManager((StandaloneResourceManager) StandaloneResourceManagerFactory.getInstance().createResourceManager(
                 config,
                 resourceId,
                 rpcService,
                 Constants.getSingleResourceManagerId(),
                 heartbeatServices,
                 delegationTokenManager,
                 throwable -> System.out.println("Fatal error occurred: " + throwable.getMessage()),
                 new ClusterInformation("localhost", 1234),
                 null,
                 createResourceManagerMetricGroup(config),
                 mockResourceManagerRuntimeServices,
                 Executors.directExecutor()));

         MockJobMaster jobMaster = MockJobMaster.builder()
                 .setResourceManager(resourceManager)
                 .setJobMasterId(jobMasterId)
                 .setJobId(jobId)
                 .setResourceManagerId(resourceManagerId)
                 .build();
        // Start ResourceManager
        resourceManager.startResourceManger();
        System.out.println("StandaloneResourceManager started...");
        System.out.println(resourceManager.getAddress());
        System.out.println("Starting JobMaster with JobID: " + jobId + " and JobMasterID: " + jobMasterId + "..." + " and ResourceManagerID: " + resourceManagerId);
        Thread.sleep(4_000);
        runJobMaster(jobMaster);
    }

    @Override
    public void stop() throws Exception {
        if (resourceManager != null) {
            resourceManager.close();
        }
    }

    private static void runJobMaster(MockJobMaster jobMaster) throws IOException {
        int numberOfSlots = Integer.parseInt(new EnvVar("NUMBER_OF_SLOTS", "2").getValue());
        jobMaster.start();
        jobMaster.submitJob(numberOfSlots);
    }

    private ResourceManagerMetricGroup createResourceManagerMetricGroup(Configuration configuration) {
        return ResourceManagerMetricGroup.create(new MetricRegistryImpl(
                        MetricRegistryConfiguration.fromConfiguration(configuration, 10000L)),
                resourceManagerAddress.getValue());
    }

    private SlotManagerMetricGroup createSlotManagerMetricGroup(Configuration configuration) {
        return SlotManagerMetricGroup.create(new MetricRegistryImpl(
                        MetricRegistryConfiguration.fromConfiguration(configuration, 10000L)),
                resourceManagerAddress.getValue());
    }

    public static class MockResourceManagerRuntimeServices extends ResourceManagerRuntimeServices {

        public MockResourceManagerRuntimeServices(ResourceManagerRuntimeServices resourceManagerRuntimeServices,
                                                  JobID jobId,
                                                  JobMasterId jobMasterId) {
            super(resourceManagerRuntimeServices.getSlotManager(), new MockJobLeaderIdService(jobId, jobMasterId));
        }
    }

    private static class MockJobLeaderIdService implements JobLeaderIdService {
        private boolean isStarted = false;

        private JobID jobId;
        private JobMasterId jobMasterId;

        private JobID addedJobId;

        public MockJobLeaderIdService(JobID jobId, JobMasterId jobMasterId) {
            this.jobId = jobId;
            this.jobMasterId = jobMasterId;
        }

        @Override
        public void start(JobLeaderIdActions initialJobLeaderIdActions) throws Exception {
            System.out.println("JobLeaderIdService started...");
            isStarted = true;
        }

        @Override
        public void stop() throws Exception {
            System.out.println("JobLeaderIdService stopped...");
            isStarted = false;
        }

        @Override
        public void clear() throws Exception {
            System.out.println("JobLeaderIdService cleared...");
        }

        @Override
        public void addJob(JobID jobId) throws Exception {
            System.out.println("JobLeaderIdService added job " + jobId + "...");
            addedJobId = jobId;

        }

        @Override
        public void removeJob(JobID jobId) throws Exception {
            if(!addedJobId.equals(jobId)) {
                System.out.println("JobLeaderIdService removed job " + jobId + "...");
            }
            addedJobId = null;
        }

        @Override
        public boolean containsJob(JobID jobId) {
            System.out.println("JobLeaderIdService contains job " + jobId + " but the registered job is " + this.jobId);
            return this.jobId.equals(jobId);
        }

        @Override
        public CompletableFuture<JobMasterId> getLeaderId(JobID jobId) throws Exception {
            return CompletableFuture.completedFuture(jobMasterId);
        }

        @Override
        public boolean isValidTimeout(JobID jobId, UUID timeoutId) {
            return true;
        }
    }

}
