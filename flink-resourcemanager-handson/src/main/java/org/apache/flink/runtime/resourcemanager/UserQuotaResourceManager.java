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
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.blocklist.BlockedNode;
import org.apache.flink.runtime.blocklist.BlocklistHandler;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.io.network.partition.DataSetMetaInfo;
import org.apache.flink.runtime.io.network.partition.ResourceManagerPartitionTrackerFactory;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.slotmanager.ResourceAllocator;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rest.messages.LogInfo;
import org.apache.flink.runtime.rest.messages.ProfilingInfo;
import org.apache.flink.runtime.rest.messages.ThreadDumpInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcServiceUtils;
import org.apache.flink.runtime.security.token.DelegationTokenManager;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.FileType;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.TaskExecutorHeartbeatPayload;
import org.apache.flink.runtime.taskexecutor.TaskExecutorThreadInfoGateway;
import org.apache.flink.runtime.taskexecutor.partition.ClusterPartitionReport;
import org.apache.flink.runtime.user.UserID;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class UserQuotaResourceManager<WorkerId extends ResourceIDRetrievable>
        extends FencedRpcEndpoint<ResourceManagerId>
        implements DelegationTokenManager.Listener, UserResourceManagerGateway {

    public static final String USER_RESOURCE_MANAGER_NAME = "userresourcemanager";

    /** Unique id of the resource manager. */
    protected final ResourceID resourceId;

    public UserQuotaResourceManager(
            RpcService rpcService,
            ResourceManagerId fencingToken,
            ResourceID resourceId) {
        super(rpcService,
                RpcServiceUtils.createRandomName(USER_RESOURCE_MANAGER_NAME),
                fencingToken);
        this.resourceId = resourceId;
    }

    /**
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
    }

    /**
     * Notify new blocked node records.
     *
     * @param newNodes the new blocked node records
     * @return Future acknowledge once the new nodes have successfully notified.
     */
    @Override
    public CompletableFuture<Acknowledge> notifyNewBlockedNodes(Collection<BlockedNode> newNodes) {
        return null;
    }

    /**
     * Returns all datasets for which partitions are being tracked.
     *
     * @return tracked datasets
     */
    @Override
    public CompletableFuture<Map<IntermediateDataSetID, DataSetMetaInfo>> listDataSets() {
        return null;
    }

    /**
     * Releases all partitions associated with the given dataset.
     *
     * @param dataSetToRelease dataset for which all associated partitions should be released
     * @return future that is completed once all partitions have been released
     */
    @Override
    public CompletableFuture<Void> releaseClusterPartitions(IntermediateDataSetID dataSetToRelease) {
        return null;
    }

    /**
     * Report the cluster partitions status in the task executor.
     *
     * @param taskExecutorId         The id of the task executor.
     * @param clusterPartitionReport The status of the cluster partitions.
     * @return future that is completed once the report have been processed.
     */
    @Override
    public CompletableFuture<Void> reportClusterPartitions(
            ResourceID taskExecutorId,
            ClusterPartitionReport clusterPartitionReport) {
        return null;
    }

    /**
     * Get the shuffle descriptors of the cluster partitions ordered by partition number.
     *
     * @param intermediateDataSetID The id of the dataset.
     * @return shuffle descriptors of the cluster partitions.
     */
    @Override
    public CompletableFuture<List<ShuffleDescriptor>> getClusterPartitionsShuffleDescriptors(
            IntermediateDataSetID intermediateDataSetID) {
        return null;
    }

    /**
     * Register a {@link JobMaster} at the resource manager.
     *
     * @param jobMasterId         The fencing token for the JobMaster leader
     * @param jobMasterResourceId The resource ID of the JobMaster that registers
     * @param jobMasterAddress    The address of the JobMaster that registers
     * @param jobId               The Job ID of the JobMaster that registers
     * @param timeout             Timeout for the future to complete
     * @return Future registration response
     */
    @Override
    public CompletableFuture<RegistrationResponse> registerJobMaster(
            JobMasterId jobMasterId,
            ResourceID jobMasterResourceId,
            String jobMasterAddress,
            JobID jobId,
            Duration timeout) {
        // Handle JobLeaderElectionID
        // Connect to JobMasterGateway
        // Validate if same JobMasterId is already registered
        // Register JobMasterGateway + JobMasterId + blovklisthandler + heartbeat
        //
        return null;
    }

    /**
     * Declares the absolute resource requirements for a job.
     *
     * @param jobMasterId          id of the JobMaster
     * @param resourceRequirements resource requirements
     * @param timeout
     * @return The confirmation that the requirements have been processed
     */
    @Override
    public CompletableFuture<Acknowledge> declareRequiredResources(
            JobMasterId jobMasterId,
            ResourceRequirements resourceRequirements,
            Duration timeout) {
        // handle if registration exists or none
        // validateRunsInMainThread then    slotManager.processResourceRequirements(
        return null;
    }

    /**
     * Register a {@link TaskExecutor} at the resource manager.
     *
     * @param taskExecutorRegistration the task executor registration.
     * @param timeout                  The timeout for the response.
     * @return The future to the response by the ResourceManager.
     */
    @Override
    public CompletableFuture<RegistrationResponse> registerTaskExecutor(
            TaskExecutorRegistration taskExecutorRegistration,
            Duration timeout) {
        return null;
    }

    /**
     * Sends the given {@link SlotReport} to the ResourceManager.
     *
     * @param taskManagerResourceId     The resource ID of the sending TaskManager
     * @param taskManagerRegistrationId id identifying the sending TaskManager
     * @param slotReport                which is sent to the ResourceManager
     * @param timeout                   for the operation
     * @return Future which is completed with {@link Acknowledge} once the slot report has been
     * received.
     */
    @Override
    public CompletableFuture<Acknowledge> sendSlotReport(
            ResourceID taskManagerResourceId,
            InstanceID taskManagerRegistrationId,
            SlotReport slotReport,
            Duration timeout) {
        return null;
    }

    /**
     * Sent by the TaskExecutor to notify the ResourceManager that a slot has become available.
     *
     * @param instanceId      TaskExecutor's instance id
     * @param slotID          The SlotID of the freed slot
     * @param oldAllocationId to which the slot has been allocated
     */
    @Override
    public void notifySlotAvailable(
            InstanceID instanceId,
            SlotID slotID,
            AllocationID oldAllocationId) {

    }

    /**
     * Deregister Flink from the underlying resource management system.
     *
     * @param finalStatus final status with which to deregister the Flink application
     * @param diagnostics additional information for the resource management system, can be {@code
     *                    null}
     */
    @Override
    public CompletableFuture<Acknowledge> deregisterApplication(
            ApplicationStatus finalStatus,
            @Nullable String diagnostics) {
        return null;
    }

    /**
     * Gets the currently registered number of TaskManagers.
     *
     * @return The future to the number of registered TaskManagers.
     */
    @Override
    public CompletableFuture<Integer> getNumberOfRegisteredTaskManagers() {
        return null;
    }

    /**
     * Sends the heartbeat to resource manager from task manager.
     *
     * @param heartbeatOrigin  unique id of the task manager
     * @param heartbeatPayload payload from the originating TaskManager
     * @return future which is completed exceptionally if the operation fails
     */
    @Override
    public CompletableFuture<Void> heartbeatFromTaskManager(
            ResourceID heartbeatOrigin,
            TaskExecutorHeartbeatPayload heartbeatPayload) {
        return null;
    }

    /**
     * Sends the heartbeat to resource manager from job manager.
     *
     * @param heartbeatOrigin unique id of the job manager
     * @return future which is completed exceptionally if the operation fails
     */
    @Override
    public CompletableFuture<Void> heartbeatFromJobManager(ResourceID heartbeatOrigin) {
        return null;
    }

    /**
     * Disconnects a TaskManager specified by the given resourceID from the {@link ResourceManager}.
     *
     * @param resourceID identifying the TaskManager to disconnect
     * @param cause      for the disconnection of the TaskManager
     */
    @Override
    public void disconnectTaskManager(ResourceID resourceID, Exception cause) {

    }

    /**
     * Disconnects a JobManager specified by the given resourceID from the {@link ResourceManager}.
     *
     * @param jobId     JobID for which the JobManager was the leader
     * @param jobStatus status of the job at the time of disconnection
     * @param cause     for the disconnection of the JobManager
     */
    @Override
    public void disconnectJobManager(JobID jobId, JobStatus jobStatus, Exception cause) {

    }

    /**
     * Requests information about the registered {@link TaskExecutor}.
     *
     * @param timeout of the request
     * @return Future collection of TaskManager information
     */
    @Override
    public CompletableFuture<Collection<TaskManagerInfo>> requestTaskManagerInfo(Duration timeout) {
        return null;
    }

    /**
     * Requests detail information about the given {@link TaskExecutor}.
     *
     * @param taskManagerId identifying the TaskExecutor for which to return information
     * @param timeout       of the request
     * @return Future TaskManager information and its allocated slots
     */
    @Override
    public CompletableFuture<TaskManagerInfoWithSlots> requestTaskManagerDetailsInfo(
            ResourceID taskManagerId,
            Duration timeout) {
        return null;
    }

    /**
     * Requests the resource overview. The resource overview provides information about the
     * connected TaskManagers, the total number of slots and the number of available slots.
     *
     * @param timeout of the request
     * @return Future containing the resource overview
     */
    @Override
    public CompletableFuture<ResourceOverview> requestResourceOverview(Duration timeout) {
        return null;
    }

    /**
     * Requests the paths for the TaskManager's {@link MetricQueryService} to query.
     *
     * @param timeout for the asynchronous operation
     * @return Future containing the collection of resource ids and the corresponding metric query
     * service path
     */
    @Override
    public CompletableFuture<Collection<Tuple2<ResourceID, String>>> requestTaskManagerMetricQueryServiceAddresses(
            Duration timeout) {
        return null;
    }

    /**
     * Request the file upload from the given {@link TaskExecutor} to the cluster's {@link
     * BlobServer}. The corresponding {@link TransientBlobKey} is returned.
     *
     * @param taskManagerId identifying the {@link TaskExecutor} to upload the specified file
     * @param fileType      type of the file to upload
     * @param timeout       for the asynchronous operation
     * @return Future which is completed with the {@link TransientBlobKey} after uploading the file
     * to the {@link BlobServer}.
     */
    @Override
    public CompletableFuture<TransientBlobKey> requestTaskManagerFileUploadByType(
            ResourceID taskManagerId,
            FileType fileType,
            Duration timeout) {
        return null;
    }

    /**
     * Request the file upload from the given {@link TaskExecutor} to the cluster's {@link
     * BlobServer}. The corresponding {@link TransientBlobKey} is returned.
     *
     * @param taskManagerId identifying the {@link TaskExecutor} to upload the specified file
     * @param fileName      name of the file to upload
     * @param fileType      type of the file to upload
     * @param timeout       for the asynchronous operation
     * @return Future which is completed with the {@link TransientBlobKey} after uploading the file
     * to the {@link BlobServer}.
     */
    @Override
    public CompletableFuture<TransientBlobKey> requestTaskManagerFileUploadByNameAndType(
            ResourceID taskManagerId,
            String fileName,
            FileType fileType,
            Duration timeout) {
        return null;
    }

    /**
     * Request log list from the given {@link TaskExecutor}.
     *
     * @param taskManagerId identifying the {@link TaskExecutor} to get log list from
     * @param timeout       for the asynchronous operation
     * @return Future which is completed with the historical log list
     */
    @Override
    public CompletableFuture<Collection<LogInfo>> requestTaskManagerLogList(
            ResourceID taskManagerId,
            Duration timeout) {
        return null;
    }

    /**
     * Requests the thread dump from the given {@link TaskExecutor}.
     *
     * @param taskManagerId taskManagerId identifying the {@link TaskExecutor} to get the thread
     *                      dump from
     * @param timeout       timeout of the asynchronous operation
     * @return Future containing the thread dump information
     */
    @Override
    public CompletableFuture<ThreadDumpInfo> requestThreadDump(
            ResourceID taskManagerId,
            Duration timeout) {
        return null;
    }

    /**
     * Requests the {@link TaskExecutorGateway}.
     *
     * @param taskManagerId identifying the {@link TaskExecutor}.
     * @param timeout
     * @return Future containing the task executor gateway.
     */
    @Override
    public CompletableFuture<TaskExecutorThreadInfoGateway> requestTaskExecutorThreadInfoGateway(
            ResourceID taskManagerId,
            Duration timeout) {
        return null;
    }

    /**
     * Request profiling list from the given {@link TaskExecutor}.
     *
     * @param taskManagerId identifying the {@link TaskExecutor} to get profiling list from
     * @param timeout       for the asynchronous operation
     * @return Future which is completed with the historical profiling list
     */
    @Override
    public CompletableFuture<Collection<ProfilingInfo>> requestTaskManagerProfilingList(
            ResourceID taskManagerId,
            Duration timeout) {
        return null;
    }

    /**
     * Requests the profiling instance from the given {@link TaskExecutor}.
     *
     * @param taskManagerId taskManagerId identifying the {@link TaskExecutor} to get the profiling
     *                      from
     * @param duration      profiling duration
     * @param mode          profiling mode {@link ProfilingMode}
     * @param timeout       timeout of the asynchronous operation
     * @return Future containing the created profiling information
     */
    @Override
    public CompletableFuture<ProfilingInfo> requestProfiling(
            ResourceID taskManagerId,
            int duration,
            ProfilingInfo.ProfilingMode mode,
            Duration timeout) {
        return null;
    }

    /**
     * Callback function when new delegation tokens obtained.
     *
     * @param tokens
     */
    @Override
    public void onNewTokensObtained(byte[] tokens) throws Exception {

    }

    /**
     * @param userID
     * @param timeout
     *
     * @return
     */
    @Override
    public CompletableFuture<Acknowledge> registerNewUser(UserID userID, Duration timeout) {
        return null;
    }

    /**
     * @param userID
     * @param resourceRequirements
     * @param timeout
     *
     * @return
     */
    @Override
    public CompletableFuture<Acknowledge> updateUserQuota(
            UserID userID,
            ResourceRequirements resourceRequirements,
            Duration timeout) {
        return null;
    }

    /**
     * @param userID
     * @param timeout
     *
     * @return
     */
    @Override
    public CompletableFuture<ResourceRequirements> getUserQuota(UserID userID, Duration timeout) {
        return null;
    }

    /**
     * @param userID
     * @param timeout
     *
     * @return
     */
    @Override
    public CompletableFuture<Acknowledge> unregisterUser(UserID userID, Duration timeout) {
        return null;
    }
}
