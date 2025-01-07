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

import org.apache.flink.rpc.handson.AbstractMain;
import org.apache.flink.rpc.handson.Constants;
import org.apache.flink.rpc.handson.EnvVar;
import org.apache.flink.runtime.jobmaster.JobMasterServiceLeadershipRunner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class ClientMain implements AbstractMain {
    private static final Logger LOG =
            LoggerFactory.getLogger(ClientMain.class);
    private ClientGateway clientGateway;

    private boolean isStarted = false;

    private EnvVar serverUrl = new EnvVar("SERVER_URL", null);
    private EnvVar serverPort = new EnvVar("SERVER_PORT", "9127");
    public ClientMain() {
        LOG.info("Starting client Main");
        System.out.println("Starting client Main");
        try {
            clientGateway = ClientFactory.INSTANCE.createClient(serverUrl.getValue(), serverPort.getValue());
            LOG.info("Client created");
            System.out.println("Client created");
            isStarted = true;
        } catch (Exception e) {
            LOG.error("Failed to create client", e);
            System.out.println("Failed to create client");
            e.printStackTrace();
        }
    }

    public void start() {
        LOG.info("Starting client main");
        System.out.println("Starting client main");
        try {
            run();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() throws Exception {
        isStarted = false;
    }

    private void run() throws InterruptedException, ExecutionException {
        clientGateway.awaitServer();

        int i = 0;
        List<CompletableFuture<?>> futureList = new ArrayList<>();
        while (isStarted) {
            Thread.sleep(100);
            if(i % 100 == 0) {
                futureList.forEach(CompletableFuture::join);
                int finalI1 = i;
                clientGateway.getState()
                        .thenApply(state -> {
                            System.out.println("Client state: " + state + " for i=" + finalI1);
                            LOG.info("Client state: {} for i={}", state, finalI1);
                            return state;
                        }).get();
            } else {
                String randomString = "test" + i;
                int finalI = i;
                futureList.add(clientGateway.alterString(randomString)
                        .thenApply(altered -> {
                            System.out.println("Altered string: " + altered + " for i=" + finalI);
                            LOG.info("Altered string: {} for i={}", altered, finalI);
                            return altered;
                        }));
            }
            i++;
        }
    }
}
