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
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.user.UserID;
import org.apache.flink.runtime.util.ResourceCounter;

import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

public class UserTaskManagerTracker extends FineGrainedTaskManagerTracker implements UserSpecificTaskmanagerTracker {

    private final Map<InstanceID, UserID> specificTaskManagers = new ConcurrentHashMap<>();

    private final Set<InstanceID> unallocatedTaskManagers = new ConcurrentSkipListSet<>();

    private final Set<InstanceID> sharedTaskManagers = new ConcurrentSkipListSet<>();;

    private final Map<PendingTaskManagerId, UserID> specificPendingTaskManagers = new ConcurrentHashMap<>();

    private final Set<PendingTaskManagerId> unallocatedPendingTaskManagers = new ConcurrentSkipListSet<>();

    private final Set<PendingTaskManagerId> sharedPendingTaskManagers = new ConcurrentSkipListSet<>();;

    public UserTaskManagerTracker() {
        super();
    }


    /**
     * @param taskManager
     * @param user
     *
     * @throws UserSlotManagerException
     */
    @Override
    public void assignTaskManagerToUser(
            TaskManagerInfo taskManager,
            UserID user) throws UserSlotManagerException {


        if (specificTaskManagers.containsKey(taskManager.getInstanceId())) {
            if (specificTaskManagers.get(taskManager.getInstanceId()).equals(user)) {
                return;
            }

            throw new UserSlotManagerException(
                    "TaskManager " + taskManager.getInstanceId() + " is already assigned to user " + specificTaskManagers.get(taskManager.getInstanceId()));
        }

        if (sharedTaskManagers.contains(taskManager.getInstanceId())) {
            throw new UserSlotManagerException(
                    "TaskManager " + taskManager.getInstanceId() + " is already assigned to a shared users");
        }

        if(!unallocatedTaskManagers.contains(taskManager.getInstanceId())) {
            throw new UserSlotManagerException(
                    "TaskManager " + taskManager.getInstanceId() + " is not available to allocation to any user");
        }

        specificTaskManagers.put(taskManager.getInstanceId(), user);
        unallocatedTaskManagers.remove(taskManager.getInstanceId());
    }

    /**
     * @param user
     *
     * @return
     */
    @Override
    public Collection<TaskManagerInfo> getSpecificTaskManagersForUser(UserID user) {
        return specificTaskManagers.entrySet()
                .stream()
                .filter(entry -> entry.getValue().equals(user))
                .map(entry -> super.getRegisteredTaskManager(entry.getKey()).get())
                .collect(Collectors.toList());
    }

    /**
     * @param user
     *
     * @return
     */
    @Override
    public Collection<TaskManagerInfo> getTaskManagersWithAllocatedSlotsForUser(UserID user) {
        return null;
    }

    /**
     * @param user
     *
     * @return
     */
    @Override
    public Collection<PendingTaskManager> getPendingTaskManagersForUser(UserID user) {
        return specificPendingTaskManagers.entrySet()
                .stream()
                .filter(entry -> entry.getValue().equals(user))
                .map(entry -> super.getPendingTaskManagers()
                        .stream()
                        .filter(pendingTaskManager -> pendingTaskManager.getPendingTaskManagerId().equals(entry.getKey()))
                        .findFirst().get())
                .collect(Collectors.toList());
    }

    /**
     * @return
     */
    @Override
    public int getNumberRegisteredSlots() {
        return super.getNumberRegisteredSlots();
    }

    /**
     * @param instanceId
     *
     * @return
     */
    @Override
    public int getNumberRegisteredSlotsOf(InstanceID instanceId) {
        return super.getNumberRegisteredSlotsOf(instanceId);
    }

    /**
     * @return
     */
    @Override
    public int getNumberFreeSlots() {
        return super.getNumberFreeSlots();
    }

    /**
     * @param instanceId
     *
     * @return
     */
    @Override
    public int getNumberFreeSlotsOf(InstanceID instanceId) {
        return super.getNumberFreeSlotsOf(instanceId);
    }

    /**
     * @return
     */
    @Override
    public ResourceProfile getRegisteredResource() {
        return super.getRegisteredResource();
    }

    /**
     * @param instanceId
     *
     * @return
     */
    @Override
    public ResourceProfile getRegisteredResourceOf(InstanceID instanceId) {
        return super.getRegisteredResourceOf(instanceId);
    }

    /**
     * @return
     */
    @Override
    public ResourceProfile getFreeResource() {
        return super.getFreeResource();
    }

    /**
     * @param instanceId
     *
     * @return
     */
    @Override
    public ResourceProfile getFreeResourceOf(InstanceID instanceId) {
        return super.getFreeResourceOf(instanceId);
    }

    /**
     * @return
     */
    @Override
    public ResourceProfile getPendingResource() {
        return super.getPendingResource();
    }

    /**
     * @return
     */
    @Override
    public Collection<? extends TaskManagerInfo> getRegisteredTaskManagers() {
        return super.getRegisteredTaskManagers();
    }

    /**
     * @param instanceId of the task manager
     *
     * @return
     */
    @Override
    public Optional<TaskManagerInfo> getRegisteredTaskManager(InstanceID instanceId) {
        return super.getRegisteredTaskManager(instanceId);
    }

    /**
     * @return
     */
    @Override
    public Collection<PendingTaskManager> getPendingTaskManagers() {
        return super.getPendingTaskManagers();
    }

    /**
     * @param allocationId of the slot
     *
     * @return
     */
    @Override
    public Optional<TaskManagerSlotInformation> getAllocatedOrPendingSlot(AllocationID allocationId) {
        return super.getAllocatedOrPendingSlot(allocationId);
    }

    /**
     * @param totalResourceProfile of the pending task manager
     * @param defaultSlotResourceProfile of the pending task manager
     *
     * @return
     */
    @Override
    public Collection<PendingTaskManager> getPendingTaskManagersByTotalAndDefaultSlotResourceProfile(
            ResourceProfile totalResourceProfile,
            ResourceProfile defaultSlotResourceProfile) {
        return super.getPendingTaskManagersByTotalAndDefaultSlotResourceProfile(totalResourceProfile, defaultSlotResourceProfile);
    }

    /**
     * @param taskExecutorConnection of the new task manager
     * @param totalResourceProfile of the new task manager
     * @param defaultSlotResourceProfile of the new task manager
     */
    @Override
    public void addTaskManager(
            TaskExecutorConnection taskExecutorConnection,
            ResourceProfile totalResourceProfile,
            ResourceProfile defaultSlotResourceProfile) {
        super.addTaskManager(taskExecutorConnection, totalResourceProfile, defaultSlotResourceProfile);
        unallocatedTaskManagers.add(taskExecutorConnection.getInstanceID());
    }

    /**
     * @param instanceId of the task manager
     */
    @Override
    public void removeTaskManager(InstanceID instanceId) {
        super.removeTaskManager(instanceId);
        specificTaskManagers.remove(instanceId);
        unallocatedTaskManagers.remove(instanceId);
        sharedTaskManagers.remove(instanceId);
    }

    /**
     * @param pendingTaskManager to be added
     */
    @Override
    public void addPendingTaskManager(PendingTaskManager pendingTaskManager) {
        super.addPendingTaskManager(pendingTaskManager);
        unallocatedPendingTaskManagers.add(pendingTaskManager.getPendingTaskManagerId());
    }

    /**
     * @param pendingTaskManagerId of the pending task manager
     *
     * @return
     */
    @Override
    public Map<JobID, ResourceCounter> removePendingTaskManager(PendingTaskManagerId pendingTaskManagerId) {
        return super.removePendingTaskManager(pendingTaskManagerId);
    }

    /**
     * @param instanceId identifier of task manager.
     */
    @Override
    public void addUnWantedTaskManager(InstanceID instanceId) {
        throw new UnsupportedOperationException("Not supported for UserTaskManagerTracker");
    }

    /**
     * @return
     */
    @Override
    public Map<InstanceID, WorkerResourceSpec> getUnWantedTaskManager() {
        return Collections.emptyMap();
    }

    /**
     * @param jobId the job for which the task executors must have a slot
     *
     * @return
     */
    @Override
    public Collection<TaskManagerInfo> getTaskManagersWithAllocatedSlotsForJob(JobID jobId) {
        return super.getTaskManagersWithAllocatedSlotsForJob(jobId);
    }

    /**
     * @param allocationId of the slot
     * @param jobId of the slot
     * @param instanceId of the slot
     * @param resourceProfile of the slot
     * @param slotState of the slot
     */
    @Override
    public void notifySlotStatus(
            AllocationID allocationId,
            JobID jobId,
            InstanceID instanceId,
            ResourceProfile resourceProfile,
            SlotState slotState) {
        System.out.println("UserTaskManagerTracker.notifySlotStatus");
        super.notifySlotStatus(allocationId, jobId, instanceId, resourceProfile, slotState);
    }

    /**
     * @param allocationId of the slot
     * @param jobId of the slot
     * @param instanceId of the slot
     * @param resourceProfile of the slot
     * @param slotState of the slot
     */
    @Override
    public void notifySlotStatus(
            AllocationID allocationId,
            JobID jobId,
            UserID userId,
            InstanceID instanceId,
            ResourceProfile resourceProfile,
            SlotState slotState) throws UserSlotManagerException {
        Preconditions.checkNotNull(allocationId);
        Preconditions.checkNotNull(jobId);
        Preconditions.checkNotNull(userId);
        Preconditions.checkNotNull(instanceId);
        Preconditions.checkNotNull(resourceProfile);
        Preconditions.checkNotNull(slotState);

        System.out.println("UserTaskManagerTracker.notifySlotStatus with userId");
        switch (slotState) {
            case FREE:
                freeSlot(instanceId, allocationId, userId);
                break;
            case ALLOCATED:
                addAllocatedSlot(allocationId, jobId, userId, instanceId, resourceProfile);
                break;
            case PENDING:
                addPendingSlot(allocationId, jobId, userId, instanceId, resourceProfile);
                break;
        }

    }

    /**
     * @param pendingSlotAllocations new pending slot allocations be recorded
     */
    @Override
    public void replaceAllPendingAllocations(Map<PendingTaskManagerId, Map<JobID, ResourceCounter>> pendingSlotAllocations) {
        super.replaceAllPendingAllocations(pendingSlotAllocations);
    }

    /**
     * @param jobId of the given job
     */
    @Override
    public void clearPendingAllocationsOfJob(JobID jobId) {
        super.clearPendingAllocationsOfJob(jobId);
    }

    @Override
    public void clearPendingAllocationsForUser(UserID userID) {
        specificPendingTaskManagers.entrySet()
                .stream()
                .filter(entry -> entry.getValue().equals(userID))
                .map(entry -> super.getPendingTaskManagers()
                        .stream()
                        .filter(pendingTaskManager -> pendingTaskManager.getPendingTaskManagerId().equals(entry.getKey()))
                        .findFirst().get())
                .forEach(pendingTaskManager -> {
                    super.removePendingTaskManager(pendingTaskManager.getPendingTaskManagerId());
                    specificPendingTaskManagers.remove(pendingTaskManager.getPendingTaskManagerId());
                });
    }

    /**
     *
     */
    @Override
    public void clear() {
        super.clear();
        specificTaskManagers.clear();
        unallocatedTaskManagers.clear();
        sharedTaskManagers.clear();
        specificPendingTaskManagers.clear();
        unallocatedPendingTaskManagers.clear();
        sharedPendingTaskManagers.clear();
    }

    private void freeSlot(InstanceID tmId, AllocationID allocationId, UserID userId) {
        super.freeSlot(tmId, allocationId);
        if(taskManagerRegistrations.get(tmId).isIdle() && sharedTaskManagers.contains(tmId)) {
            unallocatedTaskManagers.add(tmId);
            sharedTaskManagers.remove(tmId);
        }

    }

    private void addAllocatedSlot(AllocationID allocationId, JobID jobId, UserID userId, InstanceID instanceId, ResourceProfile resourceProfile) throws UserSlotManagerException {
        markTMasSharedIfNotAssigned(userId, instanceId);
        super.addAllocatedSlot(allocationId, jobId, instanceId, resourceProfile);
    }


    private void addPendingSlot(AllocationID allocationId, JobID jobId, UserID userId, InstanceID instanceId, ResourceProfile resourceProfile) throws UserSlotManagerException {
        markTMasSharedIfNotAssigned(userId, instanceId);
        super.addPendingSlot(allocationId, jobId, instanceId, resourceProfile);
    }

    private void markTMasSharedIfNotAssigned(UserID userId, InstanceID instanceId) {
        if(specificTaskManagers.containsKey(instanceId) && !specificTaskManagers.get(instanceId).equals(userId)) {
            throw new UserSlotManagerException(
                    "TaskManager " + instanceId + " is already assigned to user " + specificTaskManagers.get(instanceId));
        }

        if(unallocatedTaskManagers.contains(instanceId)) {
            unallocatedTaskManagers.remove(instanceId);
            sharedTaskManagers.add(instanceId);
        }
    }
}
