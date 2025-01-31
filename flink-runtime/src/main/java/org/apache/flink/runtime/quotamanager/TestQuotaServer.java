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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestQuotaServer {
    public static void main(String[] args) throws Exception {
        System.out.println("TestQuotaServer");
        Configuration configuration = new Configuration();

        RpcService service = RpcUtils.createRemoteRpcService(RpcSystem.load(configuration), configuration, "localhost",
               "6123" , "localhost",  Optional.of(6123));

        QuotaManagerId quotaManagerId = QuotaManagerId.generate();
        UUID leaderSessionId = UUID.randomUUID();
        ExecutorService executorService = Executors.newFixedThreadPool(
                1,
                new ExecutorThreadFactory("test-io"));
        QuotaManager<ResourceQuota> quotaManager = new QuotaManager<>(service, leaderSessionId,
                (error) -> System.out.println("Fatal error: " + error), RpcUtils.INF_TIMEOUT, executorService);

        System.out.println("Starting QuotaManager Server");
        quotaManager.start();
        System.out.println("QuotaManager Server started");

        Thread.sleep(30_000);
        QuotaUserId quotaUserId = new QuotaUserId();

        QuotaManagerGateway gateway = quotaManager.getGateway();
        gateway.updateQuota(quotaUserId, new ResourceQuota(quotaUserId, 4))
                .thenCompose((ack) -> gateway.getQuotaForUser(quotaUserId)).handle((quota, throwable) -> {
                    if (throwable != null) {
                        System.out.println("Error while updating the quota: " + throwable.getMessage());
                    } else {
                        System.out.println("Quota for user " + quotaUserId + ": " + quota.orElse(new ResourceQuota(new QuotaUserId(), 0)).getNumSlots());
                    }
                    return Acknowledge.get();
                })
                .get(30_000, java.util.concurrent.TimeUnit.MILLISECONDS);

        quotaManager.closeAsync().handle((ack, throwable) -> {
            if (throwable != null) {
                System.out.println("Error while closing the QuotaManager: " + throwable.getMessage());
            } else {
                System.out.println("QuotaManager closed");
            }
            return null;
        }).get(30_000, java.util.concurrent.TimeUnit.MILLISECONDS);



        System.out.println("QuotaManager Server closed");
    }
}
