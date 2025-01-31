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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class InmemoryQuotaClient implements QuotaProxyClient<ResourceQuota> {

    private final HashMap<QuotaUserId, ResourceQuota> userQuotas;

    private final Map<JobID, QuotaUserId> userJobs;

    public InmemoryQuotaClient(Map<QuotaUserId, ResourceQuota> quota) {
        this.userQuotas = new HashMap<>(quota);
        this.userJobs = new HashMap<>();
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public CompletableFuture<ResourceQuota> getQuotaForUser(QuotaUserId userId) {
        return CompletableFuture.completedFuture(userQuotas.get(userId));
    }

    @Override
    public CompletableFuture<Void> updateQuota(QuotaUserId userId, ResourceQuota quota) {
        userQuotas.put(userId, quota);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> removeQuota(QuotaUserId userId) {
        userQuotas.remove(userId);
        return CompletableFuture.completedFuture(null);
    }

    // UNUSED
    @Override
    public CompletableFuture<QuotaUserId> getQuotaUserForToken(String token) {
        return CompletableFuture.completedFuture(userJobs.get(JobID.fromHexString(token)));
    }
}
