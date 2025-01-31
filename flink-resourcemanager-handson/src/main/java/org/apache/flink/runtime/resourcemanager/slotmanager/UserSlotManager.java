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
import org.apache.flink.runtime.blocklist.BlockedTaskManagerChecker;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.rest.messages.taskmanager.SlotInfo;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.user.UserID;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;

public class UserSlotManager implements UserQuotaSlotManager{
    boolean running = false;


    private final TaskManagerTracker taskManagerTracker;
    private final ResourceTracker resourceTracker;
    private final ResourceAllocationStrategy resourceAllocationStrategy;

    private final SlotStatusSyncer slotStatusSyncer;

    public UserSlotManager(
            TaskManagerTracker taskManagerTracker,
            ResourceTracker resourceTracker,
            ResourceAllocationStrategy resourceAllocationStrategy,
            SlotStatusSyncer slotStatusSyncer) {
        this.taskManagerTracker = taskManagerTracker;
        this.resourceTracker = resourceTracker;
        this.resourceAllocationStrategy = resourceAllocationStrategy;
        this.slotStatusSyncer = slotStatusSyncer;
    }

    /**
     * @param userID
     */
    @Override
    public void registerNewUser(UserID userID) {

    }

    /**
     * @param userID
     * @param resourceRequirements
     */
    @Override
    public void updateUserQuota(UserID userID, ResourceRequirements resourceRequirements) {

    }

    /**
     * @param userID
     *
     * @return
     */
    @Override
    public ResourceRequirements getUserQuota(UserID userID) {
        return null;
    }

    /**
     * @param userID
     */
    @Override
    public void unregisterUser(UserID userID) {

    }

    /**
     * @return
     */
    @Override
    public List<UserID> getRegisteredUsers() {
        return null;
    }

    /**
     * @return
     */
    @Override
    public int getNumberRegisteredSlots() {
        return 0;
    }

    /**
     * @param instanceId
     *
     * @return
     */
    @Override
    public int getNumberRegisteredSlotsOf(InstanceID instanceId) {
        return 0;
    }

    /**
     * @return
     */
    @Override
    public int getNumberFreeSlots() {
        return 0;
    }

    /**
     * @param instanceId
     *
     * @return
     */
    @Override
    public int getNumberFreeSlotsOf(InstanceID instanceId) {
        return 0;
    }

    /**
     * @return
     */
    @Override
    public ResourceProfile getRegisteredResource() {
        return null;
    }

    /**
     * @param instanceID
     *
     * @return
     */
    @Override
    public ResourceProfile getRegisteredResourceOf(InstanceID instanceID) {
        return null;
    }

    /**
     * @return
     */
    @Override
    public ResourceProfile getFreeResource() {
        return null;
    }

    /**
     * @param instanceID
     *
     * @return
     */
    @Override
    public ResourceProfile getFreeResourceOf(InstanceID instanceID) {
        return null;
    }

    /**
     * @param instanceID
     *
     * @return
     */
    @Override
    public Collection<SlotInfo> getAllocatedSlotsOf(InstanceID instanceID) {
        return null;
    }

    /**
     * Starts the slot manager with the given leader id and resource manager actions.
     *
     * @param newResourceManagerId         to use for communication with the task managers
     * @param newMainThreadExecutor        to use to run code in the ResourceManager's main thread
     * @param newResourceAllocator         to use for resource (de-)allocations
     * @param resourceEventListener        to use for notify resource not enough
     * @param newBlockedTaskManagerChecker to query whether a task manager is blocked
     */
    @Override
    public void start(
            ResourceManagerId newResourceManagerId,
            Executor newMainThreadExecutor,
            ResourceAllocator newResourceAllocator,
            ResourceEventListener resourceEventListener,
            BlockedTaskManagerChecker newBlockedTaskManagerChecker) {

    }

    /**
     * Suspends the component. This clears the internal state of the slot manager.
     */
    @Override
    public void suspend() {

    }

    /**
     * Notifies the slot manager that the resource requirements for the given job should be cleared.
     * The slot manager may assume that no further updates to the resource requirements will occur.
     *
     * @param jobId job for which to clear the requirements
     */
    @Override
    public void clearResourceRequirements(JobID jobId) {

    }

    /**
     * Notifies the slot manager about the resource requirements of a job.
     *
     * @param resourceRequirements resource requirements of a job
     */
    @Override
    public void processResourceRequirements(ResourceRequirements resourceRequirements) {
        // checkInit
        // short-circuit if no requirements
        // if empty requirements, clear allocations
        // Track JM addres, WHY?
        // resourceTracker.notifyResourceRequirements
        // 1:
        //  resourceTracker.getMissingResources()
        // if empty ->   taskManagerTracker.replaceAllPendingAllocations(Collections.emptyMap());
        // resourceAllocationStrategy.tryFulfillRequirements(
        //                        missingResources, taskManagerTracker, this::isBlockedTaskManager);
        //
        // for each slot in the allocation slotStatusSyncer.allocateSlot(
        //                                        instanceID -> tm,
        //                                        jobID,
        //                                        jobMasterTargetAddresses.get(jobID),
        //                                        slotEntry.getKey()));
        // When all are done -> go to 1:
        //
        // if (resourceAllocator.isSupported()) {
        //            checkResourcesNeedReconcile();
        //            declareNeededResourcesWithDelay();
        //        }
    }

    /**
     * Registers a new task manager at the slot manager. This will make the task managers slots
     * known and, thus, available for allocation.
     *
     * @param taskExecutorConnection     for the new task manager
     * @param initialSlotReport          for the new task manager
     * @param totalResourceProfile       for the new task manager
     * @param defaultSlotResourceProfile for the new task manager
     * @return The result of task manager registration
     */
    @Override
    public RegistrationResult registerTaskManager(
            TaskExecutorConnection taskExecutorConnection,
            SlotReport initialSlotReport,
            ResourceProfile totalResourceProfile,
            ResourceProfile defaultSlotResourceProfile) {
        return null;
    }

    /**
     * Unregisters the task manager identified by the given instance id and its associated slots
     * from the slot manager.
     *
     * @param instanceId identifying the task manager to unregister
     * @param cause      for unregistering the TaskManager
     * @return True if there existed a registered task manager with the given instance id
     */
    @Override
    public boolean unregisterTaskManager(InstanceID instanceId, Exception cause) {
        return false;
    }

    /**
     * Reports the current slot allocations for a task manager identified by the given instance id.
     *
     * @param instanceId identifying the task manager for which to report the slot status
     * @param slotReport containing the status for all of its slots
     * @return true if the slot status has been updated successfully, otherwise false
     */
    @Override
    public boolean reportSlotStatus(InstanceID instanceId, SlotReport slotReport) {
        return false;
    }

    /**
     * Free the given slot from the given allocation. If the slot is still allocated by the given
     * allocation id, then the slot will be marked as free and will be subject to new slot requests.
     *
     * @param slotId       identifying the slot to free
     * @param allocationId with which the slot is presumably allocated
     */
    @Override
    public void freeSlot(SlotID slotId, AllocationID allocationId) {

    }

    /**
     * @param failUnfulfillableRequest
     */
    @Override
    public void setFailUnfulfillableRequest(boolean failUnfulfillableRequest) {

    }

    /**
     * Trigger the resource requirement check. This method will be called when some slot statuses
     * changed.
     */
    @Override
    public void triggerResourceRequirementsCheck() {

    }

    /**
     * Closes this resource, relinquishing any underlying resources.
     * This method is invoked automatically on objects managed by the
     * {@code try}-with-resources statement.
     *
     * <p>While this interface method is declared to throw {@code
     * Exception}, implementers are <em>strongly</em> encouraged to
     * declare concrete implementations of the {@code close} method to
     * throw more specific exceptions, or to throw no exception at all
     * if the close operation cannot fail.
     *
     * <p> Cases where the close operation may fail require careful
     * attention by implementers. It is strongly advised to relinquish
     * the underlying resources and to internally <em>mark</em> the
     * resource as closed, prior to throwing the exception. The {@code
     * close} method is unlikely to be invoked more than once and so
     * this ensures that the resources are released in a timely manner.
     * Furthermore it reduces problems that could arise when the resource
     * wraps, or is wrapped, by another resource.
     *
     * <p><em>Implementers of this interface are also strongly advised
     * to not have the {@code close} method throw {@link
     * InterruptedException}.</em>
     * <p>
     * This exception interacts with a thread's interrupted status,
     * and runtime misbehavior is likely to occur if an {@code
     * InterruptedException} is {@linkplain Throwable#addSuppressed
     * suppressed}.
     * <p>
     * More generally, if it would cause problems for an
     * exception to be suppressed, the {@code AutoCloseable.close}
     * method should not throw it.
     *
     * <p>Note that unlike the {@link Closeable#close close}
     * method of {@link Closeable}, this {@code close} method
     * is <em>not</em> required to be idempotent.  In other words,
     * calling this {@code close} method more than once may have some
     * visible side effect, unlike {@code Closeable.close} which is
     * required to have no effect if called more than once.
     * <p>
     * However, implementers of this interface are strongly encouraged
     * to make their {@code close} methods idempotent.
     *
     * @throws Exception if this resource cannot be closed
     */
    @Override
    public void close() throws Exception {

    }
}
