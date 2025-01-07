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

import org.apache.flink.rpc.handson.client.ClientMain;
import org.apache.flink.rpc.handson.server.ServerMain;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {

    protected static final Logger LOG = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws Exception {
        String programName = new EnvVar("PROGRAM_NAME", "server").getValue();
        LOG.info("Starting program: {}", programName);
        AbstractMain main = programName.equals("server") ? new ServerMain() : new ClientMain();
        main.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                main.stop();
            } catch (Exception e) {
                LOG.error("Failed to stop main", e);
            }
        }));
        Thread.sleep(Long.MAX_VALUE);
    }

}
