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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.rpc.handson.Constants;
import org.apache.flink.rpc.handson.EnvVar;
import org.apache.flink.runtime.highavailability.ExtendedZookeeperHaServices;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.runtime.rpc.RpcUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.Executors;

public enum ClientFactory {


    INSTANCE;


    private static final Logger LOG =
            LoggerFactory.getLogger(ClientFactory.class);

    public ClientGateway createClient(String serverHost, String serverPort) throws Exception {
        LOG.info("Creating client");
        boolean isZookeeperEnabled = isZookeeperEnabled();
        EnvVar clientUrl = new EnvVar("CLIENT_URL", "localhost");
        EnvVar clientPort = new EnvVar("CLIENT_PORT", "9125");
        Integer port = Integer.parseInt(clientPort.getValue());
        Configuration configuration = Constants.getCommonConfiguration(Constants.Role.CLIENT, isZookeeperEnabled);
        RpcService service = RpcUtils.createRemoteRpcService(
                RpcSystem.load(configuration), configuration, clientUrl.getValue(),
                clientPort.getValue() , "0.0.0.0",  Optional.of(port));
        LOG.info("Client created");
        String serverAddress = String.format("pekko.tcp://flink@%s:%s/user/rpc/%s", serverHost, serverPort, "server");
        System.out.println("Connecting to server at: " + serverAddress);
//        "pekko.tcp://flink@server:9127/user/server";

        ExtendedZookeeperHaServices haServices = isZookeeperEnabled ? createHaServices() : null;

        return new ClientImpl(service, "client", new ClientId(), serverAddress, haServices);
    }

    private boolean isZookeeperEnabled() {
        EnvVar zookeeperEnabled = new EnvVar("ZOOKEEPER_ENABLED", "true");
        boolean toret = Boolean.parseBoolean(zookeeperEnabled.getValue());
        if (toret) {
           System.out.println("Zookeeper is enabled");
        } else {
            System.out.println("Zookeeper is disabled");
        }
        return toret;
    }

    private ExtendedZookeeperHaServices createHaServices() throws Exception {
        Configuration configuration = Constants.getCommonConfiguration(Constants.Role.ZOOKEEPER, true);
        return ExtendedZookeeperHaServices.create(configuration,
                Executors.newSingleThreadExecutor());
    }
}
