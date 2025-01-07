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
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class ClientImpl extends FencedRpcEndpoint<ClientId> implements ClientGateway {

    private static final Logger LOG =
            LoggerFactory.getLogger(ClientImpl.class);
    private final ClientId clientId;
    private final String serverAddress;

    private final Supplier<ServerGateway> serverGatewaySupplier;

    protected ClientImpl(RpcService rpcService, String endpointId, ClientId fencingToken,
                         String serverAddress) {
        super(rpcService, endpointId, fencingToken);
        this.clientId = fencingToken;
        this.serverAddress = serverAddress;
        this.serverGatewaySupplier = () -> {
            try {
                System.out.println("Connecting to server at " + serverAddress);
                LOG.info("Connecting to server at {}", serverAddress);
                return rpcService.connect(serverAddress, ServerGateway.class).get();
            } catch (Exception e) {
                throw new RuntimeException("Could not connect to server", e);
            } finally {
                System.out.println("Connected to server at " + serverAddress);
                LOG.info("Connected to server at {}", serverAddress);
            }
        };
        LOG.info("Starting client");
        System.out.println("Starting client From ClientImpl");
        start();
        System.out.println("Client started From ClientImpl");
        LOG.info("Client started");
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public CompletableFuture<String> alterString(String s) {
        LOG.info("Client altering string: {}", s);
        System.out.println("Client altering string: " + s);
        return serverGatewaySupplier.get().processString(s);
    }

    @Override
    public CompletableFuture<String> getState() {
        System.out.println("Client getting state");
        LOG.info("Client getting state");
        return serverGatewaySupplier.get().getServerStatus();
    }

    @Override
    public CompletableFuture<Void> awaitServer() {
        boolean available = false;
        while (!available) {
            try {
                Thread.sleep(1000);
                System.out.println("Waiting for server to be available");
                serverGatewaySupplier.get();
                System.out.println("Server is available");
                available = true;
            } catch (RuntimeException | InterruptedException e) {
                System.out.println("Server not available");
            }
        }
        return CompletableFuture.completedFuture(null);
    }
}
