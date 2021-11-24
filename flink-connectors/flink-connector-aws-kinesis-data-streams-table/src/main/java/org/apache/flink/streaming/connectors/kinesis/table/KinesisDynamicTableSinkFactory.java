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
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.kinesis.sink.KinesisDataStreamsSinkElementConverter.PartitionKeyGenerator;
import org.apache.flink.streaming.connectors.kinesis.table.utils.KinesisConnectorOptionsGeneralUtils;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.options.AsyncSinkConnectorOptions;
import org.apache.flink.table.connector.sink.AsyncDynamicTableSinkFactory;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/** Factory for creating {@link KinesisDynamicTableSink} instance. */
@Internal
public abstract class KinesisDynamicTableSinkFactory extends AsyncDynamicTableSinkFactory
        implements DynamicTableSinkFactory {

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {

        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig tableOptions = helper.getOptions();
        ResolvedCatalogTable catalogTable = context.getCatalogTable();
        DataType physicalDataType = catalogTable.getResolvedSchema().toPhysicalRowDataType();

        KinesisConnectorOptionsGeneralUtils optionsUtils =
                new KinesisConnectorOptionsGeneralUtils(
                        catalogTable.getOptions(),
                        tableOptions,
                        (RowType) physicalDataType.getLogicalType(),
                        catalogTable.getPartitionKeys(),
                        context.getClassLoader());

        // initialize the table format early in order to register its consumedOptionKeys
        // in the TableFactoryHelper, as those are needed for correct option validation
        EncodingFormat<SerializationSchema<RowData>> encodingFormat =
                helper.discoverEncodingFormat(SerializationFormatFactory.class, FactoryUtil.FORMAT);

        // validate the data types of the table options
        helper.validateExcept(optionsUtils.getNonValidatedPrefixes().toArray(new String[0]));
        // Validate option values
        validateKinesisPartitioner(tableOptions, catalogTable);

        Properties properties = optionsUtils.getValidatedSinkConfigurations();

        KinesisDynamicTableSink.KinesisDynamicTableSinkBuilder builder =
                new KinesisDynamicTableSink.KinesisDynamicTableSinkBuilder();

        builder.setStream((String) properties.get(KinesisConnectorOptions.STREAM));
        builder.setKinesisClientProperties(
                (Properties)
                        properties.get(
                                KinesisConnectorOptionsGeneralUtils.KINESIS_CLIENT_PROPERTIES_KEY));
        builder.setEncodingFormat(encodingFormat);
        builder.setConsumedDataType(physicalDataType);
        builder.setPartitioner(
                (PartitionKeyGenerator<RowData>)
                        properties.get(KinesisConnectorOptions.SINK_PARTITIONER.key()));

        if (properties.containsKey(AsyncSinkConnectorOptions.FLUSH_BUFFER_SIZE.key())) {
            builder.setFlushOnBufferSizeInBytes(
                    (Long) properties.get(AsyncSinkConnectorOptions.FLUSH_BUFFER_SIZE.key()));
        }

        if (properties.containsKey(AsyncSinkConnectorOptions.FLUSH_BUFFER_TIMEOUT.key())) {
            builder.setMaxTimeInBufferMS(
                    (Long) properties.get(AsyncSinkConnectorOptions.FLUSH_BUFFER_TIMEOUT.key()));
        }

        if (properties.containsKey(AsyncSinkConnectorOptions.MAX_BATCH_SIZE.key())) {
            builder.setMaxBatchSize(
                    (Integer) properties.get(AsyncSinkConnectorOptions.MAX_BATCH_SIZE.key()));
        }

        if (properties.containsKey(AsyncSinkConnectorOptions.MAX_BUFFERED_REQUESTS.key())) {
            builder.setMaxBufferedRequests(
                    (Integer)
                            properties.get(AsyncSinkConnectorOptions.MAX_BUFFERED_REQUESTS.key()));
        }

        if (properties.containsKey(AsyncSinkConnectorOptions.MAX_IN_FLIGHT_REQUESTS.key())) {
            builder.setMaxInFlightRequests(
                    (Integer)
                            properties.get(AsyncSinkConnectorOptions.MAX_IN_FLIGHT_REQUESTS.key()));
        }

        if (properties.containsKey(KinesisConnectorOptions.SINK_FAIL_ON_ERROR.key())) {
            builder.setFailOnError(
                    (Boolean) properties.get(KinesisConnectorOptions.SINK_FAIL_ON_ERROR.key()));
        }

        return builder.build();
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(KinesisConnectorOptions.STREAM);
        options.add(FactoryUtil.FORMAT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = super.optionalOptions();
        options.add(KinesisConnectorOptions.SINK_PARTITIONER);
        options.add(KinesisConnectorOptions.SINK_PARTITIONER_FIELD_DELIMITER);
        options.add(KinesisConnectorOptions.SINK_FAIL_ON_ERROR);
        return options;
    }

    private static void validateKinesisPartitioner(
            ReadableConfig tableOptions, CatalogTable targetTable) {
        tableOptions
                .getOptional(KinesisConnectorOptions.SINK_PARTITIONER)
                .ifPresent(
                        partitioner -> {
                            if (targetTable.isPartitioned()) {
                                throw new ValidationException(
                                        String.format(
                                                "Cannot set %s option for a table defined with a PARTITIONED BY clause",
                                                KinesisConnectorOptions.SINK_PARTITIONER.key()));
                            }
                        });
    }
}
