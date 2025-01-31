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

package org.apache.flink.runtime;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.taskexecutor.TaskExecutorMemoryConfiguration;

import java.util.UUID;

public class Constants {

    public enum Role {
        TASK_EXECUTOR,
        RESOURCE_MANAGER,
        JOB_MANAGER,
        TASK_MANAGER
    }
    public static Configuration getCommonConfiguration(Role role, boolean isHaEnabled) {
        Configuration configuration = new Configuration();
        switch (role) {
            case TASK_EXECUTOR:
                return overrideWithMemory(configuration, 0);
            case RESOURCE_MANAGER:
                break;
        }
        return configuration;
    }

    private static Configuration overrideWithMemory(Configuration configuration, int memory) {
        configuration.set(TaskManagerOptions.FRAMEWORK_HEAP_MEMORY, TaskManagerOptions.FRAMEWORK_HEAP_MEMORY.defaultValue());
        configuration.set(TaskManagerOptions.TASK_HEAP_MEMORY, MemorySize.parse("2g"));
        configuration.set(TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY, TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY.defaultValue());
        configuration.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, TaskManagerOptions.TASK_OFF_HEAP_MEMORY.defaultValue());
        configuration.set(TaskManagerOptions.NETWORK_MEMORY_MAX, TaskManagerOptions.NETWORK_MEMORY_MIN.defaultValue());
        configuration.set(TaskManagerOptions.NETWORK_MEMORY_MIN, TaskManagerOptions.NETWORK_MEMORY_MIN.defaultValue());
        configuration.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.ofMebiBytes(0));
        configuration.set(TaskManagerOptions.JVM_OVERHEAD_MAX, TaskManagerOptions.JVM_OVERHEAD_MIN.defaultValue());
        configuration.set(TaskManagerOptions.JVM_OVERHEAD_MIN, TaskManagerOptions.JVM_OVERHEAD_MIN.defaultValue());
        configuration.set(TaskManagerOptions.JVM_METASPACE, TaskManagerOptions.JVM_METASPACE.defaultValue());
        return configuration;
    }


    public static UUID getSingleResourceManagerId() {
        return UUID.fromString("d45fb8c5-a95a-4691-a3b7-1363cccbad27");
    }

}
