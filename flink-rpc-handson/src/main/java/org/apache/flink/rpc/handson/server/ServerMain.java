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
import org.apache.flink.runtime.highavailability.ExtendedZookeeperHaServices;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.runtime.rpc.RpcUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;

public class ServerMain implements AbstractMain {
    private static final Logger LOG =
            LoggerFactory.getLogger(ServerMain.class);

    private EnvVar zookeeperUrl = new EnvVar("ZOOKEEPER_URL", "zookeeper:2181");
    private EnvVar serverUrl = new EnvVar("SERVER_URL", "localhost");

    private ServerRunner serverService;

    public ServerMain() throws Exception {
        LOG.info("Starting server Main");
        EnvVar zookeeperEnabled = new EnvVar("ZOOKEEPER_ENABLED", "true");
        if (Boolean.parseBoolean(zookeeperEnabled.getValue())) {
            serverService = new ServiceBasedServerRunner();
        } else {
            serverService = new StandAloneServerRunner();
        }
    }

    @Override
    public void start() throws Exception {
        LOG.info("Starting server");
        System.out.println("Starting server");
        serverService.start();
        LOG.info("Server started");
        System.out.println("Server started");
    }

    @Override
    public void stop() throws Exception {
        LOG.info("Stopping server");
        System.out.println("Stopping server");
        serverService.stop();
        LOG.info("Server stopped");
        System.out.println("Server stopped");
    }

    private interface  ServerRunner {
        void start() throws Exception;
        void stop() throws Exception;
    }

    private static class StandAloneServerRunner implements ServerRunner {
        private final Server server;

        public StandAloneServerRunner() throws Exception {
            EnvVar serverUrl = new EnvVar("SERVER_URL", "localhost");
            EnvVar serverPort = new EnvVar("SERVER_PORT", "9127");
            System.out.println("Creating StandAloneServerRunner with host: " + serverUrl.getValue() + " and port: " + serverPort.getValue());
            this.server = new ServerFactoryImpl(false).createServer(ServerId.fromUUID(UUID.randomUUID()));
        }

        @Override
        public void start() throws Exception {
            System.out.println("Starting StandAloneServerRunner");
            server.start();
            System.out.println("StandAloneServerRunner started");
        }

        @Override
        public void stop() throws Exception {
            System.out.println("Stopping StandAloneServerRunner");
            server.close();
            System.out.println("StandAloneServerRunner stopped");
        }
    }

    private static class ServiceBasedServerRunner implements ServerRunner {

        private ServerService service;

        public ServiceBasedServerRunner() throws Exception {
            Configuration configuration = Constants.getCommonConfiguration(Constants.Role.ZOOKEEPER, true);
            ExtendedZookeeperHaServices zookeeperHaServices = ExtendedZookeeperHaServices.create(configuration,
                    Executors.newSingleThreadExecutor());
            ServerFactory serverFactory = new ServerFactoryImpl(true);
            service = new ServerServiceImpl(zookeeperHaServices, serverFactory);
        }

        @Override
        public void start() throws Exception {
            System.out.println("Starting ServiceBasedServerRunner");
            service.start();
            System.out.println("ServiceBasedServerRunner started");
        }

        @Override
        public void stop() throws Exception {
            System.out.println("Stopping ServiceBasedServerRunner");
            service.close();
            System.out.println("ServiceBasedServerRunner stopped");
        }
    }


    private static class ServerFactoryImpl implements ServerFactory {

        private final boolean isHaEnabled;
        public ServerFactoryImpl(boolean isHaEnabled) {
            this.isHaEnabled = isHaEnabled;
        }

        @Nonnull
        @Override
        public Server createServer(ServerId serverId) throws Exception {
            EnvVar serverUrl = new EnvVar("SERVER_URL", "localhost");
            EnvVar serverPort = new EnvVar("SERVER_PORT", "9127");
            Integer port = Integer.parseInt(serverPort.getValue());
            Configuration configuration = Constants.getCommonConfiguration(Constants.Role.SERVER, isHaEnabled);
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
            return new Server(service, serverId, "server");
        }
    }
}
