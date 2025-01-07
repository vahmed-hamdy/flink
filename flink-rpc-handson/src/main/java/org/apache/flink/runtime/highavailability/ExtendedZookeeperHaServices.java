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

package org.apache.flink.runtime.highavailability;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.blob.BlobUtils;
import org.apache.flink.runtime.highavailability.zookeeper.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.flink.runtime.highavailability.zookeeper.ZooKeeperLeaderElectionHaServices;
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.util.ZooKeeperUtils;

import java.util.concurrent.Executor;

public class ExtendedZookeeperHaServices extends ZooKeeperLeaderElectionHaServices {

    private static final String SERVER_NODE = "server";
    public ExtendedZookeeperHaServices(
            CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper,
            Configuration configuration,
            Executor executor,
            BlobStoreService blobStoreService) throws Exception {
        super(curatorFrameworkWrapper, configuration, executor, blobStoreService);
    }

    public LeaderRetrievalService createServerLeaderElectionService() {
        return createLeaderRetrievalService(SERVER_NODE);
    }

    public LeaderElection getServerLeaderElection() {
        return leaderElectionService.createLeaderElection(SERVER_NODE);
    }


    public static ExtendedZookeeperHaServices create(Configuration configuration,Executor executor) throws Exception {
        BlobStoreService blobStoreService = BlobUtils.createBlobStoreFromConfig(configuration);

        final CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper =
                ZooKeeperUtils.startCuratorFramework(configuration, (err) -> System.out.println("Unhandled error in ZooKeeper connection: " + err));
        return new ExtendedZookeeperHaServices(curatorFrameworkWrapper, configuration, executor, blobStoreService);
    }
}
