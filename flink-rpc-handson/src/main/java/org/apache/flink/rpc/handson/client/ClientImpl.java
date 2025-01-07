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

package org.apache.flink.rpc.handson.client;

import org.apache.flink.rpc.handson.server.ServerGateway;
import org.apache.flink.rpc.handson.server.ServerId;
import org.apache.flink.runtime.highavailability.ExtendedZookeeperHaServices;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class ClientImpl extends FencedRpcEndpoint<ClientId> implements ClientGateway {

    private static final Logger LOG =
            LoggerFactory.getLogger(ClientImpl.class);
    private final ClientId clientId;
    private String serverAddress;

    private UUID leaderServerSessionID;


    private boolean isConnected = false;

    private final LeaderRetrievalService serverLeaderRetrievalService;

    protected ClientImpl(RpcService rpcService, String endpointId, ClientId fencingToken,
                         @Nullable String serverAddress, @Nullable ExtendedZookeeperHaServices haServices) throws Exception {
        super(rpcService, endpointId, fencingToken);
        this.clientId = fencingToken;
        this.serverAddress = serverAddress;
        this.leaderServerSessionID = null;
        if (haServices != null) {
            this.serverLeaderRetrievalService = haServices.createServerLeaderElectionService();
            this.serverLeaderRetrievalService.start(new ServerLeaderRetrievalListener());
        } else {
            this.serverLeaderRetrievalService = null;
        }

        LOG.info("Starting client");
        System.out.println("Starting client From ClientImpl");
        start();
        System.out.println("Client started From ClientImpl");
        LOG.info("Client started");
    }

    private ServerGateway getServerGateway() {
        try {
            System.out.println("Connecting to server at " + serverAddress);
            LOG.info("Connecting to server at {}", serverAddress);
            return serverAddress == null ?
                    new NoopServerGateway() :
                    getRpcService().connect(serverAddress, ServerId.fromUUID(leaderServerSessionID), ServerGateway.class).get();
        } catch (Exception e) {
            throw new RuntimeException("Could not connect to server", e);
        } finally {
            System.out.println("Connected to server at " + serverAddress);
            LOG.info("Connected to server at {}", serverAddress);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public CompletableFuture<String> alterString(String s) {
        LOG.info("Client altering string: {}", s);
        System.out.println("Client altering string: " + s);
        return getServerGateway().processString(s);
    }

    @Override
    public CompletableFuture<String> getState() {
        System.out.println("Client getting state");
        LOG.info("Client getting state");
        try {
            return CompletableFuture.completedFuture(getServerGateway().getServerStatus().get());
        } catch (InterruptedException | ExecutionException e) {
            System.out.println("Error getting state " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<Void> awaitServer() {
        boolean available = false;
        while (!available) {
            try {
                Thread.sleep(1000);
                System.out.println("Waiting for server to be available");
                getServerGateway();
                System.out.println("Server is available");
                available = true;
            } catch (RuntimeException | InterruptedException e) {
                System.out.println("Server not available");
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    private void disconnectFromServer(@Nullable Exception e) {
        System.out.println("Disconnecting from server " + (e != null ? e.getMessage() : ""));
        isConnected = false;
    }

    private void redefineServerAddress(String serverAddress, UUID leaderSessionID) {
        System.out.println("Redefining server address to " + serverAddress + " with session ID " + leaderSessionID);
        if (this.serverAddress != null && !this.serverAddress.equals(serverAddress)) {
            disconnectFromServer(null);
        }

        if(serverAddress != null) {
            isConnected = true;
            this.serverAddress = serverAddress;
            this.leaderServerSessionID = leaderSessionID;
        }
    }

    protected class ServerLeaderRetrievalListener implements LeaderRetrievalListener {
        @Override
        public void notifyLeaderAddress(
                @Nullable String leaderAddress,
                @Nullable UUID leaderSessionID) {
            redefineServerAddress(leaderAddress, leaderSessionID);
        }

        @Override
        public void handleError(Exception exception) {
            disconnectFromServer(exception);
        }
    }

    protected static class NoopServerGateway implements ServerGateway {
        @Override
        public CompletableFuture<String> processString(String s) {
            return CompletableFuture.completedFuture("No server available to process string");
        }

        @Override
        public CompletableFuture<String> getServerStatus() {
            return CompletableFuture.completedFuture("No server status available");
        }

        @Override
        public ServerId getFencingToken() {
            return null;
        }

        @Override
        public String getAddress() {
            return "No server address available";
        }

        @Override
        public String getHostname() {
            return "No server hostname available";
        }
    }
}
