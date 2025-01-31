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

public class QuotaExceededException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public QuotaExceededException(QuotaUserId message) {
        super("Quota exceeded for user " + message);
    }


    public QuotaExceededException(QuotaUserId message, ResourceQuota quota, int numRequestedSlots) {
        super("Quota exceeded for user " + message + ". Quota: " + quota.getNumSlots() + " slots, " +
                "requested: " + numRequestedSlots + " slots.");
    }
}
