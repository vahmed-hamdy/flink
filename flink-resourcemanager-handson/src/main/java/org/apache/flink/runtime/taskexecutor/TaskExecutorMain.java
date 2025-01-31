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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.AbstractMain;
import org.apache.flink.runtime.Constants;
import org.apache.flink.runtime.EnvVar;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.runtime.rpc.RpcUtils;

import java.time.Duration;
import java.util.Optional;

public class TaskExecutorMain implements AbstractMain {
    MockTaskExecutor taskExecutor;


    @Override
    public void start() throws Exception {
        EnvVar tasxExecutorUrl = new EnvVar("TASK_EXECUTOR_URL", "localhost");
        EnvVar taskExecutorPort = new EnvVar("TASK_EXECUTOR_PORT", "6127");

        EnvVar rmExecutorUrl = new EnvVar("RESOURCE_MANAGER_URL", "localhost");
        EnvVar rmExecutorPort = new EnvVar("RESOURCE_MANAGER_PORT", "6123");
        Integer port = Integer.parseInt(taskExecutorPort.getValue());
        Configuration configuration = Constants.getCommonConfiguration(Constants.Role.TASK_EXECUTOR, false);
        RpcSystem rpcSystem = RpcSystem.load(configuration);
        System.out.println("Creating task executor with host: " + tasxExecutorUrl.getValue() + " and port: " + port);
        RpcService service = RpcUtils.createRemoteRpcService(
                rpcSystem,
                configuration,
                tasxExecutorUrl.getValue(),
                String.valueOf(port),
                "0.0.0.0",
                Optional.of(port)
        );

        String resourceManagerAddress = String.format("pekko.tcp://flink@%s:%s/user/rpc/%s", rmExecutorUrl.getValue(),
                rmExecutorPort.getValue(), "resourcemanager_1");
        // Instantiate the mock task executor
        taskExecutor = new MockTaskExecutor(service, resourceManagerAddress);
        if(!taskExecutor.awaitResourceManagerReadiness(Duration.ofMinutes(5))) {
            throw new RuntimeException("Resource manager did not become available within 5 minutes");
        }

        taskExecutor.initialize();

        System.out.println("Mock TaskExecutor started on port " + port);

    }

    @Override
    public void stop() throws Exception {
        taskExecutor.close();

    }
}
