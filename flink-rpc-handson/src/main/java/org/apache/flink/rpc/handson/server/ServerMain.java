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
import org.apache.flink.rpc.handson.AbstractMain;
import org.apache.flink.rpc.handson.Constants;
import org.apache.flink.rpc.handson.EnvVar;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.runtime.rpc.RpcUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class ServerMain implements AbstractMain {
    private static final Logger LOG =
            LoggerFactory.getLogger(ServerMain.class);

    private EnvVar zookeeperUrl = new EnvVar("ZOOKEEPER_URL", "zookeeper:2181");
    private EnvVar serverUrl = new EnvVar("SERVER_URL", "localhost");

    private Server server;

    public ServerMain() throws Exception {
        LOG.info("Starting server Main");
        System.out.println("Starting server Main");
        EnvVar serverPort = new EnvVar("SERVER_PORT", "9127");
        Integer port = Integer.parseInt(serverPort.getValue());
        Configuration configuration = Constants.getCommonConfiguration(true);
        RpcSystem rpcSystem = RpcSystem.load(configuration);
        System.out.println("Creating server with host: " + serverUrl.getValue() + " and port: " + port);
        RpcService service = RpcUtils.createRemoteRpcService(
                rpcSystem,
                configuration,
                serverUrl.getValue(),
                String.valueOf(port),
                "0.0.0.0",
                Optional.of(port)
        );
        server = new Server(service, "server");
        System.out.println("Server Started at: " + server.getAddress());
    }
    @Override
    public void start() throws Exception {
        LOG.info("Starting server");
        System.out.println("Starting server");
        server.start();
        LOG.info("Server started");
        System.out.println("Server started");
    }

    @Override
    public void stop() throws Exception {
        LOG.info("Stopping server");
        System.out.println("Stopping server");
        server.close();
        LOG.info("Server stopped");
        System.out.println("Server stopped");
    }
}
