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

package org.apache.flink.rpc.handson.server;

import org.apache.flink.rpc.handson.Unused;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerServiceImpl;

import org.apache.flink.runtime.rpc.FatalErrorHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

@Unused(reason = "This is a placeholder for the actual server service")
public class ServerServiceImpl implements ServerService, LeaderContender{
    private static final Logger LOG = LoggerFactory.getLogger(ResourceManagerServiceImpl.class);


//    private final LeaderElection leaderElection;
//
//    private final FatalErrorHandler fatalErrorHandler;
//    private final Executor ioExecutor;
//
//    private final ExecutorService handleLeaderEventExecutor;
//    private final CompletableFuture<Void> serviceTerminationFuture;

    private final Object lock = new Object();

    @GuardedBy("lock")
    private boolean running;

    @Nullable
    @GuardedBy("lock")
    private Server leaderServer;

    @Override
    public CompletableFuture<Void> closeAsync() {
        return null;
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing server service.");
    }

    @Override
    public ServerGateway getGateway() {
        synchronized (lock) {
            if (leaderServer == null) {
                return null;
            }
            return null;
        }
    }

    @Override
    public String getAddress() {
        synchronized (lock) {
            if (leaderServer == null) {
                return null;
            }
            return leaderServer.getAddress();
        }
    }

    @Override
    public CompletableFuture<Void> getTerminationFuture() {
        return null;
    }

    @Override
    public void grantLeadership(UUID leaderSessionID) {

    }

    @Override
    public void revokeLeadership() {

    }

    @Override
    public void handleError(Exception exception) {

    }
}
