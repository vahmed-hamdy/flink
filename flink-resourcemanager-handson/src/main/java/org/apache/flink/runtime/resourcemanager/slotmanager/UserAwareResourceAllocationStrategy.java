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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.blocklist.BlockedTaskManagerChecker;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.slots.ResourceRequirement;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;

public class UserAwareResourceAllocationStrategy implements ResourceAllocationStrategy {
    private final ResourceProfile defaultSlotResourceProfile;
    private final ResourceProfile totalResourceProfile;
    private final int numSlotsPerWorker;
    private final CPUResource minTotalCPU;
    private final MemorySize minTotalMemory;

    private final int redundantTaskManagerNum;

    private final DefaultResourceAllocationStrategy.ResourceMatchingStrategy resourceMatchingStrategy;

    public UserAwareResourceAllocationStrategy(ResourceProfile totalResourceProfile,
                                               int numSlotsPerWorker,
                                               TaskManagerOptions.TaskManagerLoadBalanceMode taskManagerLoadBalanceMode,
                                               int redundantTaskManagerNum,
                                               CPUResource minTotalCPU,
                                               MemorySize minTotalMemory) {
        this.totalResourceProfile = totalResourceProfile;
        this.numSlotsPerWorker = numSlotsPerWorker;
        this.minTotalCPU = minTotalCPU;
        this.minTotalMemory = minTotalMemory;
        this.redundantTaskManagerNum = redundantTaskManagerNum;
        this.defaultSlotResourceProfile = SlotManagerUtils.generateDefaultSlotResourceProfile(totalResourceProfile, numSlotsPerWorker);
        this.resourceMatchingStrategy = taskManagerLoadBalanceMode == TaskManagerOptions.TaskManagerLoadBalanceMode.SLOTS
                ? DefaultResourceAllocationStrategy.LeastUtilizationResourceMatchingStrategy.INSTANCE
                : DefaultResourceAllocationStrategy.AnyMatchingResourceMatchingStrategy.INSTANCE;
    }


    /**
     * @param missingResources resource requirements that are not yet fulfilled, indexed by jobId
     * @param taskManagerResourceInfoProvider provide the registered/pending resources of the
     *         current cluster
     * @param blockedTaskManagerChecker blocked task manager checker
     *
     * @return
     */
    @Override
    public ResourceAllocationResult tryFulfillRequirements(
            Map<JobID, Collection<ResourceRequirement>> missingResources,
            TaskManagerResourceInfoProvider taskManagerResourceInfoProvider,
            BlockedTaskManagerChecker blockedTaskManagerChecker) {
        if(! (taskManagerResourceInfoProvider instanceof UserTaskManagerResourceInfoProvider)) {
            throw new IllegalArgumentException("taskManagerResourceInfoProvider should be an instance of UserTaskManagerResourceInfoProvider");
        }

        UserTaskManagerResourceInfoProvider userTaskManagerResourceInfoProvider = (UserTaskManagerResourceInfoProvider) taskManagerResourceInfoProvider;
        final ResourceAllocationResult.Builder resultBuilder = ResourceAllocationResult.builder();



        return null;
    }

    /**
     * @param taskManagerResourceInfoProvider provide the registered/pending resources of the
     *         current cluster
     *
     * @return
     */
    @Override
    public ResourceReconcileResult tryReconcileClusterResources(TaskManagerResourceInfoProvider taskManagerResourceInfoProvider) {
        return null;
    }
}
