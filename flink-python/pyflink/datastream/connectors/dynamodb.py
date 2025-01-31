################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
from enum import Enum
from typing import Any, Callable, Dict, Union, List

from pyflink.common.typeinfo import TypeInformation
from pyflink.datastream.connectors import Sink
from pyflink.java_gateway import get_gateway
from py4j.java_gateway import get_java_class
from pyflink.util.java_utils import to_jarray

__all__ = [
    'DynamoDbSink',
    'DynamoDbSinkBuilder'
]


class DynamoDbSink(Sink):
    """
    A DynamoDb (DDB) Sink that performs async requests against a DDB table using the buffering protocol.
    """

    def __init__(self, j_dynamodb_sink):
        super(DynamoDbSink, self).__init__(sink=j_dynamodb_sink)

    @staticmethod
    def builder() -> 'DynamoDbSinkBuilder':
        return DynamoDbSinkBuilder()


class DynamoDbSinkBuilder(object):
    def __init__(self):
        JDynamoDbSink = get_gateway().jvm.org.apache.flink.connector.dynamodb.sink.DynamoDbSink
        self._j_dynamodb_sink_builder = JDynamoDbSink.builder()

    def set_table_name(self, table_name: str) -> 'DynamoDbSinkBuilder':
        """
        Sets the name of the DynamoDb table name that the sink will connect to. There is no default
        for this parameter, therefore, this must be provided at sink creation time otherwise the
        build will fail.
        """
        self._j_dynamodb_sink_builder.setTableName(table_name)
        return self

    def set_element_converter(self, element_converter: ElementConverter) \
        -> 'DynamoDbSinkBuilder':
        """
        Sets converter to convert expected element to DynamoDbWriteRequest handled by DynamoDb client.
        """
        self._j_dynamodb_sink_builder.setElementConverter(element_converter.to_j_element_converter())
        return self

    def set_overwrite_by_partition_keys(self, *overwrite_by_partition_keys: str) -> 'DynamoDbSinkBuilder':
        """
        overwriteByPartitionKeys is a list of attribute key names for the sink to deduplicate on if
        you want to bypass the no duplication limitation of a single batch write request.
        Batching DynamoDB sink will drop request items in the buffer if their primary
        keys(composite) values are the same as the newly added ones. The newer request item in a
        single batch takes precedence.
        """
        j_overwrite_by_partition_keys_arr = to_jarray(get_gateway().jvm.java.lang.String, overwrite_by_partition_keys)
        j_overwrite_by_partition_keys = get_gateway().jvm.java.util.Arrays.asList(j_overwrite_by_partition_keys_arr)
        self._j_dynamodb_sink_builder.setOverwriteByPartitionKeys(j_overwrite_by_partition_keys)
        return self

    def set_fail_on_error(self, fail_on_error: bool) -> 'DynamoDbSinkBuilder':
        """
        If writing to DynamoDb Table results in a partial or full failure being returned,
        the job will fail
        """
        self._j_dynamodb_sink_builder.setFailOnError(fail_on_error)
        return self

    def set_dynamodb_client_properties(self, dynamodb_client_properties: Dict) \
        -> 'DynamoDbSinkBuilder':
        """
        A set of properties used by the sink to create the dynamodb client. This may be used to set
        the aws region, credentials etc. See the docs for usage and syntax.
        """
        j_properties = get_gateway().jvm.java.util.Properties()
        for key, value in dynamodb_client_properties.items():
            j_properties.setProperty(key, value)
        self._j_dynamodb_sink_builder.setDynamoDbProperties(j_properties)
        return self

    def set_max_batch_size(self, max_batch_size: int) -> 'DynamoDbSinkBuilder':
        """
        Maximum number of elements that may be passed in a list to be written downstream.
        """
        self._j_dynamodb_sink_builder.setMaxBatchSize(max_batch_size)
        return self

    def set_max_in_flight_requests(self, max_in_flight_requests: int) \
        -> 'DynamoDbSinkBuilder':
        """
        Maximum number of uncompleted calls to submitRequestEntries that the SinkWriter will allow
        at any given point. Once this point has reached, writes and callbacks to add elements to
        the buffer may block until one or more requests to submitRequestEntries completes.
        """
        self._j_dynamodb_sink_builder.setMaxInFlightRequests(max_in_flight_requests)
        return self

    def set_max_buffered_requests(self, max_buffered_requests: int) -> 'DynamoDbSinkBuilder':
        """
        The maximum buffer length. Callbacks to add elements to the buffer and calls to write will
        block if this length has been reached and will only unblock if elements from the buffer have
        been removed for flushing.
        """
        self._j_dynamodb_sink_builder.setMaxBufferedRequests(max_buffered_requests)
        return self

    def set_max_time_in_buffer_ms(self, max_time_in_buffer_ms: int) -> 'DynamoDbSinkBuilder':
        """
        The maximum amount of time an element may remain in the buffer. In most cases elements are
        flushed as a result of the batch size (in bytes or number) being reached or during a
        snapshot. However, there are scenarios where an element may remain in the buffer forever or
        a long period of time. To mitigate this, a timer is constantly active in the buffer such
        that: while the buffer is not empty, it will flush every maxTimeInBufferMS milliseconds.
        """
        self._j_dynamodb_sink_builder.setMaxTimeInBufferMS(max_time_in_buffer_ms)
        return self

    def build(self) -> 'DynamoDbSink':
        """
        Build thd DynamoDbSink.
        """
        return DynamoDbSink(self._j_dynamodb_sink_builder.build())
