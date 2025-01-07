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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.highavailability.ExtendedZookeeperHaServices;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.resourcemanager.ResourceManagerServiceImpl;

import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ServerServiceImpl implements ServerService, LeaderContender {
    private static final Logger LOG = LoggerFactory.getLogger(ResourceManagerServiceImpl.class);

    private final ExtendedZookeeperHaServices highAvailabilityServices;

    private final LeaderElection leaderElection;

    private final Object lock = new Object();

    private CompletableFuture<Void> terminationFuture;

    private final ExecutorService handleLeaderEventExecutor;

    private final ServerFactory serverFactory;


    @GuardedBy("lock")
    private boolean running;

    @Nullable
    @GuardedBy("lock")
    private Server leaderServer;

    @Nullable
    @GuardedBy("lock")
    private ServerId leaderServerId;

    public ServerServiceImpl(ExtendedZookeeperHaServices highAvailabilityServices,
                             ServerFactory serverFactory) {
        this.highAvailabilityServices = highAvailabilityServices;
        this.leaderElection = highAvailabilityServices.getServerLeaderElection();
        this.serverFactory = serverFactory;
        this.terminationFuture = new CompletableFuture<>();
        this.handleLeaderEventExecutor = Executors.newSingleThreadExecutor();
        leaderServer = null;
        leaderServerId = null;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        FutureUtils.forward(CompletableFuture.runAsync(
                () -> {
                    try {
                        LOG.info("Closing server service.");
                        revokeLeadership();
                    } catch (Exception e) {
                        LOG.error("Error while closing server service.", e);
                    }
                },
                handleLeaderEventExecutor), terminationFuture);
        return terminationFuture;
    }


    @Override
    public void start() throws Exception {
        boolean wasRunning = withLock(() -> {
            if (running) {
                return true;
            }
            running = true;
            return false;
        });

        if (wasRunning) {
            return;
        }

        leaderElection.startLeaderElection(this);
    }

    @Override
    public ServerGateway getGateway() {
        return withLock(() -> leaderServer);
    }

    @Override
    public String getAddress() {
        return withLock(() -> {
            if (leaderServer == null) {
                return null;
            }
            return leaderServer.getAddress();
        });
    }

    @Override
    public CompletableFuture<Void> getTerminationFuture() {
        return terminationFuture;
    }

    @Override
    public void grantLeadership(UUID leaderSessionID) {
        withLock(() -> grantLeadershipInternal(ServerId.fromUUID(leaderSessionID)));
    }

    @Override
    public void revokeLeadership() {
        withLock(this::revokeLeaderShipInternal);
    }

    @GuardedBy("lock")
    private void grantLeadershipInternal(ServerId leaderSessionID) throws Exception {
        System.out.println("Granting leadership for server with id " + leaderSessionID);
        if(Objects.equals(leaderServerId, leaderSessionID)) {
            System.out.println("Server already granted leadership with id " + leaderSessionID);
            return;
        }

        if(leaderServer != null) {
            System.out.println("Closing existing leader server with id " + leaderServerId.toString());
            leaderServer.close();
        }


        leaderServer = serverFactory.createServer(leaderSessionID);
        leaderServerId = leaderSessionID;
        System.out.println("Starting leader server with id " + leaderSessionID);
        leaderServer.start();
        System.out.println("Server granted leadership with id " + leaderSessionID);
        // NOTE: Confirm leadership, this is really important to notify clients that the server is the leader
        leaderElection.confirmLeadershipAsync(leaderSessionID.toUUID(), getAddress());
    }


    @GuardedBy("lock")
    private void revokeLeaderShipInternal() throws Exception {
        System.out.println("Revoking leadership for server with id " + leaderServerId.toString());
        if (leaderServer != null) {
            leaderServer.close();
            leaderServer = null;
        } else {
            System.out.println("No leader server to revoke leadership from.");
        }

        leaderServerId = null;
    }

    @Override
    public void handleError(Exception exception) {
        System.out.println("Error in leader election: " + exception.getMessage());
        System.err.println("Error in leader election: " + exception.getMessage());
    }

    private <T> T withLock(Callable<T> callable) {
        synchronized (lock) {
            try {
                return callable.call();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void withLock(ThrowingRunnable runnable) {
        synchronized (lock) {
            try {
                runnable.runUnSafe();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private  interface ThrowingRunnable {
        void runUnSafe() throws Exception;
    }
}
