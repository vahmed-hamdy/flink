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

package org.apache.flink.runtime.quotamanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcServiceUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class QuotaManager<QuotaT extends ResourceQuota> extends FencedRpcEndpoint<QuotaManagerId>
        implements QuotaManagerGateway, QuotaManagerService {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private static final List<QuotaUserId> registeredUsers = Arrays.asList(
            new QuotaUserId(),
            new QuotaUserId()
    );

    private Random random = new Random();

    private static final String QUOTA_MANAGER_NAME = "quota_manager";

    // TODO: Add metrics
    private final Map<QuotaUserId, QuotaT> userQuotaCache;

    private final FatalErrorHandler fatalErrorHandler;

    private final Duration rpcTimeout;

    private final Executor ioExecutor;

    private final QuotaProxyClient<QuotaT> quotaProxyClient;


    @SuppressWarnings("unchecked")
    protected QuotaManager(
            RpcService rpcService,
            UUID leaderSessionId,
            FatalErrorHandler fatalErrorHandler,
            Duration rpcTimeout,
            Executor ioExecutor) {
        super(rpcService,
                RpcServiceUtils.createRandomName(QUOTA_MANAGER_NAME),
                QuotaManagerId.fromUuid(leaderSessionId));

        this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
        this.rpcTimeout = checkNotNull(rpcTimeout);
        this.ioExecutor = checkNotNull(ioExecutor);
        this.userQuotaCache = new HashMap<>();
        // TODO: Add factory to create QuotaProxyClient
        this.quotaProxyClient = (QuotaProxyClient<QuotaT>) new InmemoryQuotaClient(new HashMap<>());
        log.info("Quota manager started.");
        log.info("Registered users: {}", registeredUsers);

    }

    @Override
    public CompletableFuture<Optional<ResourceQuota>> getQuotaForUser(QuotaUserId userId) {
        return CompletableFuture.completedFuture(Optional.ofNullable(userQuotaCache.get(userId)));
    }

    @SuppressWarnings("unchecked")
    @Override
    public CompletableFuture<Acknowledge> updateQuota(QuotaUserId userId, ResourceQuota quota) {
        return this.quotaProxyClient.updateQuota(userId, (QuotaT) quota).thenCompose(ack -> {
            userQuotaCache.put(userId, (QuotaT) quota);
            return CompletableFuture.completedFuture(Acknowledge.get());
        });
    }

    @Override
    public CompletableFuture<Acknowledge> removeQuota(QuotaUserId userId) {
        return this.quotaProxyClient.removeQuota(userId).thenCompose(ack -> {
            userQuotaCache.remove(userId);
            return CompletableFuture.completedFuture(Acknowledge.get());
        });
    }

    @Override
    public CompletableFuture<Acknowledge> allocateAvailableSlots(QuotaUserId userId, int numSlots) throws QuotaExceededException {
        log.info("Allocating {} slots for user {}", numSlots, userId);
        return this.quotaProxyClient.getQuotaForUser(userId).thenApply(quota -> {
            if (quota.getNumSlots() < numSlots) {
                throw new QuotaExceededException(userId);
            }
            return Acknowledge.get();
        });
    }

    @Override
    public CompletableFuture<Optional<QuotaUserId>> getUserForJob(JobMasterId jobId) {
        int index = random.nextInt(registeredUsers.size());
        log.info("User for job {} is {}", jobId, registeredUsers.get(index));
        return CompletableFuture.completedFuture(Optional.of(registeredUsers.get(index)));
    }

    @Override
    public void close() throws Exception {
        this.quotaProxyClient.stop();
    }

    @Override
    public QuotaManagerGateway getGateway() {
        return getSelfGateway(QuotaManagerGateway.class);
    }
}
