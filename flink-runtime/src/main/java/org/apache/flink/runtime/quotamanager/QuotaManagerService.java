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

package org.apache.flink.runtime.quotamanager;

import org.apache.flink.util.AutoCloseableAsync;

import java.util.concurrent.CompletableFuture;

/**
 * Interface which specifies the QuotaManager service.
 */
public interface QuotaManagerService extends AutoCloseableAsync {

        /**
        * Get the {@link QuotaManagerGateway} belonging to this service.
        *
        * @return QuotaManagerGateway belonging to this service
        */
        QuotaManagerGateway getGateway();

        /**
        * Get the address of the QuotaManager service under which it is reachable.
        *
        * @return Address of the QuotaManager service
        */
        String getAddress();

    /**
     * Get the termination future of this quota manager service.
     * @return
     */

        CompletableFuture<Void> getTerminationFuture();
}
