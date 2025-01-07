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

package org.apache.flink.rpc.handson;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.rpc.handson.server.ServerGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.runtime.rpc.RpcUtils;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class Dummy {

    public static void main(String[] args) throws Exception {
        testServerRpcConnection();
    }

    private static void testServerRpcConnection() throws Exception {
        Configuration config = new Configuration();
        RpcService rpcService = RpcUtils.createRemoteRpcService(
                RpcSystem.load(config),
                config,
                "localhost",
                "9123",
                "0.0.0.0",
                Optional.of(9123)
        );

        String serverAddress = "pekko.tcp://flink@localhost:9127/user/rpc/server";

        System.out.println("Connecting to server at: " + serverAddress);

        CompletableFuture<ServerGateway> serverGatewayFuture =
                rpcService.connect(serverAddress, ServerGateway.class);

        ServerGateway serverGateway = serverGatewayFuture.get();

        CompletableFuture<String> response = serverGateway.processString("Hello, Server!");
        System.out.println("Server response: " + response.get());
    }
}
