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

import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;

import java.util.concurrent.CompletableFuture;

public class Server extends FencedRpcEndpoint<ServerId> implements ServerGateway {
    private final ServerProxy proxy;

    private ServerId serverId;

    protected Server(RpcService rpcService, ServerId serverId, String endpointId) {
        super(rpcService, endpointId, serverId);
        this.serverId = serverId;
        this.proxy = new ServerProxy(serverId, getAddress());
    }

    @Override
    public void onStart() {
        System.out.println("FencedRpcEndpoint started with token: " + getFencingToken() + "and address " + getAddress());
    }

    @Override
    public CompletableFuture<Void> onStop() {
        System.out.println("FencedRpcEndpoint stopping.");
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void close() throws Exception {
        log.info("Closing server with id {}", serverId);
        System.out.println("Closing server with id " + serverId);
    }

    @Override
    public CompletableFuture<String> processString(String s) {
        System.out.println("Processing string " + s + " on server with id " + serverId);
        log.info("Processing string {} on server with id {}", s, serverId);
        return CompletableFuture.completedFuture(proxy.processStringAndUpdateState(s));
    }

    @Override
    public CompletableFuture<String> getServerStatus() {
        System.out.println("Getting status of server with id " + serverId);
        log.info("Getting status of server with id {}", serverId);
        return CompletableFuture.completedFuture(proxy.getState());
    }

    @Override
    public ServerId getFencingToken() {
        return serverId;
    }
}
