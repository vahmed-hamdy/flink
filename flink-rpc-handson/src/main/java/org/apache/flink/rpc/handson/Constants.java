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

public class Constants {

    public static Configuration getCommonConfiguration(boolean isServer) {
        EnvVar serverUrl = new EnvVar("SERVER_URL", "localhost");
        EnvVar serverPort = new EnvVar("SERVER_PORT", "9127");
        EnvVar clientPort = new EnvVar("CLIENT_PORT", "9125");
        EnvVar clientAddress = new EnvVar("CLIENT_ADDRESS", "localhost");
        Configuration conf =  getCommonConfiguration(serverUrl.getValue(), serverPort.getValue(), clientPort.getValue(), clientAddress.getValue());
        if (isServer) {
            conf.setString("rpc.address", serverUrl.getValue());
            conf.setString("rpc.bind.address", serverUrl.getValue());
            conf.setString("rpc.port", serverPort.getValue());
        } else {
            conf.setString("rpc.address", clientAddress.getValue());
            conf.setString("rpc.bind.address", serverUrl.getValue());
            conf.setString("rpc.port", clientPort.getValue());
        }
        return conf;
    }
    public static Configuration getCommonConfiguration(String serverUrl, String port, String clientPort, String clientAddress) {
        Configuration configuration = new Configuration();
        configuration.setString("rpc.server.port", port);
        configuration.setString("rpc.server.address", serverUrl);
        configuration.setString("rpc.server.external-address", serverUrl);
        configuration.setString("rpc.server.external-port-range", port);
        configuration.setString("rpc.client.address", clientAddress);
        configuration.setString("rpc.client.port", clientPort);
        configuration.setString("io.tmp.dirs", "/tmp");
        return configuration;
    }
}
