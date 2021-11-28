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

package org.apache.flink.streaming.connectors.kinesis.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.kinesis.sink.KinesisDataStreamsSinkElementConverter;
import org.apache.flink.streaming.connectors.kinesis.table.utils.AWSOptionsUtils;
import org.apache.flink.streaming.connectors.kinesis.table.utils.KinesisClientOptionsUtils;
import org.apache.flink.table.AsyncSinkConnectorOptions;
import org.apache.flink.table.connector.options.ConfigurationValidator;
import org.apache.flink.table.connector.options.TableOptionsUtils;
import org.apache.flink.table.connector.sink.options.AsyncSinkConfigurationValidator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Class for handling kinesis table options, including key mapping and validations and property
 * extraction. Class uses options decorators {@link AWSOptionsUtils}, {@link
 * KinesisClientOptionsUtils} and {@link KinesisConsumerOptionsUtils} for handling each specified
 * set of options.
 */
@Internal
public class KinesisConnectorOptionsGeneralUtils extends AsyncSinkConfigurationValidator {

    private static final Logger LOG =
            LoggerFactory.getLogger(KinesisConnectorOptionsGeneralUtils.class);
    private final AWSOptionsUtils awsOptionsUtils;
    private final KinesisClientOptionsUtils kinesisClientOptionsUtils;
    private final KinesisConsumerOptionsUtils kinesisConsumerOptionsUtils;
    private final KinesisProducerOptionsMapper kinesisProducerOptionsMapper;
    private final Map<String, String> resolvedOptions;
    private final ReadableConfig tableOptions;
    private final KinesisDataStreamsSinkElementConverter.PartitionKeyGenerator<RowData> partitioner;

    public static final String KINESIS_CLIENT_PROPERTIES_KEY = "sink.client.properties";

    /** Options handled and validated by the table-level layer. */
    private static final Set<String> TABLE_LEVEL_OPTIONS =
            new HashSet<>(
                    Arrays.asList(
                            KinesisConnectorOptions.STREAM.key(),
                            FactoryUtil.FORMAT.key(),
                            KinesisConnectorOptions.SINK_PARTITIONER.key(),
                            KinesisConnectorOptions.SINK_FAIL_ON_ERROR.key(),
                            KinesisConnectorOptions.SINK_PARTITIONER_FIELD_DELIMITER.key(),
                            AsyncSinkConnectorOptions.FLUSH_BUFFER_SIZE.key(),
                            AsyncSinkConnectorOptions.FLUSH_BUFFER_TIMEOUT.key(),
                            AsyncSinkConnectorOptions.MAX_BATCH_SIZE.key(),
                            AsyncSinkConnectorOptions.MAX_BUFFERED_REQUESTS.key(),
                            AsyncSinkConnectorOptions.MAX_IN_FLIGHT_REQUESTS.key()));

    /**
     * Prefixes of properties that are validated by downstream components and should not be
     * validated by the Table API infrastructure.
     */
    private static final String[] NON_VALIDATED_PREFIXES =
            new String[] {
                AWSOptionsUtils.AWS_PROPERTIES_PREFIX,
                KinesisClientOptionsUtils.SINK_CLIENT_PREFIX,
                KinesisConsumerOptionsUtils.CONSUMER_PREFIX,
                KinesisProducerOptionsMapper.KINESIS_PRODUCER_PREFIX
            };

    public KinesisConnectorOptionsGeneralUtils(
            Map<String, String> options,
            ReadableConfig tableOptions,
            RowType physicalType,
            List<String> partitionKeys,
            ClassLoader classLoader) {
        super(tableOptions);
        this.resolvedOptions = new HashMap<>();
        // filtering out Table level options as they are handled by factory utils.
        options.entrySet().stream()
                .filter(entry -> !TABLE_LEVEL_OPTIONS.contains(entry.getKey()))
                .forEach(entry -> resolvedOptions.put(entry.getKey(), entry.getValue()));

        this.tableOptions = tableOptions;
        this.awsOptionsUtils = new AWSOptionsUtils(resolvedOptions);
        this.kinesisClientOptionsUtils = new KinesisClientOptionsUtils(resolvedOptions);
        this.kinesisConsumerOptionsUtils =
                new KinesisConsumerOptionsUtils(
                        resolvedOptions, tableOptions.get(KinesisConnectorOptions.STREAM));
        this.kinesisProducerOptionsMapper = new KinesisProducerOptionsMapper(resolvedOptions);
        this.partitioner =
                KinesisPartitionKeyGeneratorFactory.getKinesisPartitioner(
                        tableOptions, physicalType, partitionKeys, classLoader);
    }

    public Properties getValidatedSourceConfigurations() {
        return kinesisConsumerOptionsUtils.getValidatedConfigurations();
    }

    public Properties getValidatedSinkConfigurations() {
        Properties properties = super.getValidatedConfigurations();
        properties.put(
                KinesisConnectorOptions.STREAM, tableOptions.get(KinesisConnectorOptions.STREAM));
        Properties awsProps = awsOptionsUtils.getValidatedConfigurations();
        Properties kinesisClientProps = kinesisClientOptionsUtils.getValidatedConfigurations();
        Properties producerFallbackProperties =
                kinesisProducerOptionsMapper.getValidatedConfigurations();

        for (Map.Entry<Object, Object> entry : awsProps.entrySet()) {
            if (!properties.containsKey(entry.getKey())) {
                kinesisClientProps.put(entry.getKey(), entry.getValue());
            }
        }

        for (Map.Entry<Object, Object> entry : producerFallbackProperties.entrySet()) {
            if (!properties.containsKey(entry.getKey())) {
                properties.put(entry.getKey(), entry.getValue());
            }
        }

        properties.put(KINESIS_CLIENT_PROPERTIES_KEY, kinesisClientProps);
        properties.put(KinesisConnectorOptions.SINK_PARTITIONER.key(), this.partitioner);

        if (tableOptions.getOptional(KinesisConnectorOptions.SINK_FAIL_ON_ERROR).isPresent()) {
            properties.put(
                    KinesisConnectorOptions.SINK_FAIL_ON_ERROR.key(),
                    tableOptions.getOptional(KinesisConnectorOptions.SINK_FAIL_ON_ERROR).get());
        }
        if (!awsProps.containsKey(AWSConfigConstants.AWS_REGION)) {
            // per requirement in Amazon Kinesis DataStream
            throw new IllegalArgumentException(
                    String.format(
                            "For FlinkKinesisSink AWS region ('%s') must be set in the config.",
                            AWSConfigConstants.AWS_REGION));
        }
        return properties;
    }

    public List<String> getNonValidatedPrefixes() {
        return Arrays.asList(NON_VALIDATED_PREFIXES);
    }

    @Override
    public Properties getValidatedConfigurations() {
        Properties properties = getValidatedSourceConfigurations();
        Properties sinkProperties = getValidatedSinkConfigurations();
        for (Map.Entry<Object, Object> entry : sinkProperties.entrySet()) {
            if (!properties.containsKey(entry.getKey())) {
                properties.put(entry.getKey(), entry.getValue());
            }
        }
        return properties;
    }

    private static class KinesisProducerOptionsMapper
            implements TableOptionsUtils, ConfigurationValidator {
        private static final String KINESIS_PRODUCER_PREFIX = "sink.producer.";
        private static final Map<String, String> kinesisProducerFallbackKeys = new HashMap<>();

        static {
            kinesisProducerFallbackKeys.put(
                    "sink.producer.record-max-buffered-time",
                    AsyncSinkConnectorOptions.FLUSH_BUFFER_TIMEOUT.key());
            kinesisProducerFallbackKeys.put(
                    "sink.producer.collection-max-size",
                    AsyncSinkConnectorOptions.MAX_BATCH_SIZE.key());
            kinesisProducerFallbackKeys.put(
                    "sink.producer.collection-max-count",
                    AsyncSinkConnectorOptions.MAX_IN_FLIGHT_REQUESTS.key());
            kinesisProducerFallbackKeys.put(
                    "sink.producer.fail-on-error",
                    KinesisConnectorOptions.SINK_FAIL_ON_ERROR.key());
        }

        private final Map<String, String> resolvedOptions;

        public KinesisProducerOptionsMapper(Map<String, String> resolvedOptions) {
            this.resolvedOptions = resolvedOptions;
        }

        @Override
        public Properties getValidatedConfigurations() {
            Properties properties = new Properties();
            properties.putAll(getProcessedResolvedOptions());
            if (properties.containsKey(AsyncSinkConnectorOptions.FLUSH_BUFFER_TIMEOUT.key())) {
                ConfigurationValidator.validateOptionalPositiveLongProperty(
                        properties,
                        AsyncSinkConnectorOptions.FLUSH_BUFFER_TIMEOUT.key(),
                        properties.getProperty(
                                AsyncSinkConnectorOptions.FLUSH_BUFFER_TIMEOUT.key()));
                properties.put(
                        AsyncSinkConnectorOptions.FLUSH_BUFFER_TIMEOUT.key(),
                        Long.parseLong(
                                properties.getProperty(
                                        AsyncSinkConnectorOptions.FLUSH_BUFFER_TIMEOUT.key())));
            }

            if (properties.containsKey(AsyncSinkConnectorOptions.MAX_BATCH_SIZE.key())) {
                ConfigurationValidator.validateOptionalPositiveIntProperty(
                        properties,
                        AsyncSinkConnectorOptions.MAX_BATCH_SIZE.key(),
                        properties.getProperty(AsyncSinkConnectorOptions.MAX_BATCH_SIZE.key()));
                properties.put(
                        AsyncSinkConnectorOptions.MAX_BATCH_SIZE.key(),
                        Integer.parseInt(
                                properties.getProperty(
                                        AsyncSinkConnectorOptions.MAX_BATCH_SIZE.key())));
            }

            if (properties.containsKey(AsyncSinkConnectorOptions.MAX_IN_FLIGHT_REQUESTS.key())) {
                ConfigurationValidator.validateOptionalPositiveIntProperty(
                        properties,
                        AsyncSinkConnectorOptions.MAX_IN_FLIGHT_REQUESTS.key(),
                        properties.getProperty(
                                AsyncSinkConnectorOptions.MAX_IN_FLIGHT_REQUESTS.key()));
                properties.put(
                        AsyncSinkConnectorOptions.MAX_IN_FLIGHT_REQUESTS.key(),
                        Integer.parseInt(
                                properties.getProperty(
                                        AsyncSinkConnectorOptions.MAX_IN_FLIGHT_REQUESTS.key())));
            }

            if (properties.containsKey(KinesisConnectorOptions.SINK_FAIL_ON_ERROR.key())) {
                ConfigurationValidator.validateOptionalBooleanProperty(
                        properties,
                        KinesisConnectorOptions.SINK_FAIL_ON_ERROR.key(),
                        properties.getProperty(KinesisConnectorOptions.SINK_FAIL_ON_ERROR.key()));
                properties.put(
                        KinesisConnectorOptions.SINK_FAIL_ON_ERROR.key(),
                        Boolean.parseBoolean(
                                properties.getProperty(
                                        KinesisConnectorOptions.SINK_FAIL_ON_ERROR.key())));
            }
            return properties;
        }

        @Override
        public Map<String, String> getProcessedResolvedOptions() {
            Map<String, String> processedResolvedOptions = new HashMap<>();
            for (String key : resolvedOptions.keySet()) {
                if (key.startsWith(KINESIS_PRODUCER_PREFIX)) {
                    if (kinesisProducerFallbackKeys.containsKey(key)) {
                        processedResolvedOptions.put(
                                kinesisProducerFallbackKeys.get(key), resolvedOptions.get(key));
                    } else {
                        LOG.warn(
                                String.format(
                                        "Key %s is unsupported by Kinesis Datastream Sink", key));
                    }
                }
            }
            return processedResolvedOptions;
        }

        @Override
        public List<String> getNonValidatedPrefixes() {
            return Collections.singletonList(KINESIS_PRODUCER_PREFIX);
        }
    }
}
