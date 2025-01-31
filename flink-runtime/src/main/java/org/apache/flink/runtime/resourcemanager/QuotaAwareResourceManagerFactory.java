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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blocklist.BlocklistUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.io.network.partition.ResourceManagerPartitionTrackerImpl;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.security.token.DelegationTokenManager;
import org.apache.flink.util.ConfigurationException;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executor;

public class QuotaAwareResourceManagerFactory extends ResourceManagerFactory<ResourceID>{
    @Override
    protected ResourceManager<ResourceID> createResourceManager(
            Configuration configuration,
            ResourceID resourceId,
            RpcService rpcService,
            UUID leaderSessionId,
            HeartbeatServices heartbeatServices,
            DelegationTokenManager delegationTokenManager,
            FatalErrorHandler fatalErrorHandler,
            ClusterInformation clusterInformation,
            @Nullable String webInterfaceUrl,
            ResourceManagerMetricGroup resourceManagerMetricGroup,
            ResourceManagerRuntimeServices resourceManagerRuntimeServices,
            Executor ioExecutor) throws Exception {
        return new QuotaAwareResourceManager(
                rpcService,
                leaderSessionId,
                resourceId,
                heartbeatServices,
                delegationTokenManager,
                resourceManagerRuntimeServices.getSlotManager(),
                ResourceManagerPartitionTrackerImpl::new,
                BlocklistUtils.loadBlocklistHandlerFactory(configuration),
                resourceManagerRuntimeServices.getJobLeaderIdService(),
                clusterInformation,
                fatalErrorHandler,
                resourceManagerMetricGroup,
                Duration.ofMillis(60_000L),
                ioExecutor);
    }

    @Override
    protected ResourceManagerRuntimeServicesConfiguration createResourceManagerRuntimeServicesConfiguration(
            Configuration configuration) throws ConfigurationException {
        return ResourceManagerRuntimeServicesConfiguration.fromConfiguration(
                configuration, ArbitraryWorkerResourceSpecFactory.INSTANCE);
    }
}
