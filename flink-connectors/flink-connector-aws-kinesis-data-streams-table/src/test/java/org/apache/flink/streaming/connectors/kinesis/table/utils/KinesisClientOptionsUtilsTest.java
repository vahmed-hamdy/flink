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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/** Unit tests for {@link KinesisClientOptionsUtils}. */
public class KinesisClientOptionsUtilsTest {

    @Rule public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testGoodKinesisClientOptionsMapping() {
        Map<String, String> defaultClientOptions = getDefaultClientOptions();
        KinesisClientOptionsUtils kinesisClientOptionsUtils =
                new KinesisClientOptionsUtils(defaultClientOptions);
        Map<String, String> expectedConfigurations = getDefaultExpectedClientOptions();
        Map<String, String> actualConfigurations =
                kinesisClientOptionsUtils.getProcessedResolvedOptions();
        assertEquals(expectedConfigurations, actualConfigurations);
    }

    @Test
    public void testGoodKinesisClientOptionsSelectionAndMapping() {
        Map<String, String> defaultClientOptions = getDefaultClientOptions();
        defaultClientOptions.put("sink.not.client.some.option", "someValue");
        KinesisClientOptionsUtils kinesisClientOptionsUtils =
                new KinesisClientOptionsUtils(defaultClientOptions);
        Map<String, String> expectedConfigurations = getDefaultExpectedClientOptions();
        Map<String, String> actualConfigurations =
                kinesisClientOptionsUtils.getProcessedResolvedOptions();
        assertEquals(expectedConfigurations, actualConfigurations);
    }

    @Test
    public void testGoodKinesisClientConfigurations() {
        Map<String, String> defaultClientOptions = getDefaultClientOptions();
        KinesisClientOptionsUtils kinesisClientOptionsUtils =
                new KinesisClientOptionsUtils(defaultClientOptions);
        Properties expectedConfigurations = getDefaultExpectedClientConfigs();
        Properties actualConfigurations = kinesisClientOptionsUtils.getValidatedConfigurations();
        assertEquals(expectedConfigurations, actualConfigurations);
    }

    @Test
    public void testBadKinesisClientMaxConcurrency() {
        Map<String, String> defaultClientOptions = getDefaultClientOptions();
        defaultClientOptions.put("sink.http-client.max-concurrency", "invalid-integer");
        KinesisClientOptionsUtils kinesisClientOptionsUtils =
                new KinesisClientOptionsUtils(defaultClientOptions);
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(
                "Invalid value given for HTTP client max concurrency. Must be positive integer.");
        kinesisClientOptionsUtils.getValidatedConfigurations();
    }

    @Test
    public void testBadKinesisClientReadTimeout() {
        Map<String, String> defaultClientOptions = getDefaultClientOptions();
        defaultClientOptions.put("sink.http-client.read-timeout", "invalid-integer");
        KinesisClientOptionsUtils kinesisClientOptionsUtils =
                new KinesisClientOptionsUtils(defaultClientOptions);
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(
                "Invalid value given for HTTP read timeout. Must be positive integer.");
        kinesisClientOptionsUtils.getValidatedConfigurations();
    }

    @Test
    public void testBadKinesisClientHttpVersion() {
        Map<String, String> defaultProperties = getDefaultClientOptions();
        defaultProperties.put("sink.http-client.protocol.version", "invalid-http-protocol");
        KinesisClientOptionsUtils kinesisClientOptionsUtils =
                new KinesisClientOptionsUtils(defaultProperties);
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Invalid value given for HTTP protocol. Must be HTTP1_1 or HTTP2.");
        kinesisClientOptionsUtils.getValidatedConfigurations();
    }

    private static Map<String, String> getDefaultClientOptions() {
        Map<String, String> defaultKinesisClientOptions = new HashMap<String, String>();
        defaultKinesisClientOptions.put("sink.http-client.max-concurrency", "10000");
        defaultKinesisClientOptions.put("sink.http-client.read-timeout", "360000");
        defaultKinesisClientOptions.put("sink.http-client.protocol.version", "HTTP2");
        return defaultKinesisClientOptions;
    }

    private static Map<String, String> getDefaultExpectedClientOptions() {
        Map<String, String> defaultExpectedKinesisClientConfigurations =
                new HashMap<String, String>();
        defaultExpectedKinesisClientConfigurations.put(
                "flink.stream.kinesis.http-client.max-concurrency", "10000");
        defaultExpectedKinesisClientConfigurations.put(
                "flink.stream.kinesis.http-client.read-timeout", "360000");
        defaultExpectedKinesisClientConfigurations.put("aws.http.protocol.version", "HTTP2");
        return defaultExpectedKinesisClientConfigurations;
    }

    private static Properties getDefaultExpectedClientConfigs() {
        Properties defaultExpectedKinesisClientConfigurations = new Properties();
        defaultExpectedKinesisClientConfigurations.put(
                "flink.stream.kinesis.http-client.max-concurrency", "10000");
        defaultExpectedKinesisClientConfigurations.put(
                "flink.stream.kinesis.http-client.read-timeout", "360000");
        defaultExpectedKinesisClientConfigurations.put("aws.http.protocol.version", "HTTP2");
        return defaultExpectedKinesisClientConfigurations;
    }
}
