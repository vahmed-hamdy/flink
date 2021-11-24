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

package org.apache.flink.streaming.connectors.kinesis.table.utils;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.util.KinesisConfigGeneralUtil;
import org.apache.flink.table.connector.options.ConfigurationValidator;
import org.apache.flink.table.connector.options.GeneralOptionsUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** Handler for AWS specific table options. */
@PublicEvolving
public class AWSOptionsUtils implements GeneralOptionsUtils, ConfigurationValidator {

    private final Map<String, String> resolvedOptions;

    public AWSOptionsUtils(Map<String, String> resolvedOptions) {
        this.resolvedOptions = resolvedOptions;
    }

    /**
     * Prefix for properties defined in {@link
     * org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants}.
     */
    public static final String AWS_PROPERTIES_PREFIX = "aws.";

    @Override
    public Map<String, String> getProcessedResolvedOptions() {
        Map<String, String> mappedResolvedOptions = new HashMap<>();
        for (String key : resolvedOptions.keySet()) {
            if (key.startsWith(AWS_PROPERTIES_PREFIX)) {
                mappedResolvedOptions.put(translateAwsKey(key), resolvedOptions.get(key));
            }
        }
        return mappedResolvedOptions;
    }

    @Override
    public List<String> getNonValidatedPrefixes() {
        return Collections.singletonList(AWS_PROPERTIES_PREFIX);
    }

    @Override
    public Properties getValidatedConfigurations() {
        Properties awsConfigurations = new Properties();
        Map<String, String> mappedProperties = getProcessedResolvedOptions();
        for (Map.Entry<String, String> entry : mappedProperties.entrySet()) {
            awsConfigurations.setProperty(entry.getKey(), entry.getValue());
        }
        validateAwsConfiguration(awsConfigurations);
        ConfigurationValidator.validateOptionalBooleanProperty(
                awsConfigurations,
                AWSConfigConstants.TRUST_ALL_CERTIFICATES,
                String.format(
                        "Invalid %s value, must be a boolean.",
                        AWSConfigConstants.TRUST_ALL_CERTIFICATES));
        return awsConfigurations;
    }

    /** Map {@code scan.foo.bar} to {@code flink.foo.bar}. */
    private static String translateAwsKey(String key) {
        if (!key.endsWith("credentials.provider")) {
            return key.replace("credentials.", "credentials.provider.");
        } else {
            return key;
        }
    }

    /** Validate configuration properties related to Amazon AWS service. */
    public static void validateAwsConfiguration(Properties config) {
        KinesisConfigGeneralUtil.validateAwsConfiguration(config);
    }
}
