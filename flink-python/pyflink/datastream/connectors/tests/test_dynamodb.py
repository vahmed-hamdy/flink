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
from typing import Any, Callable

from pyflink.common import Types
from pyflink.datastream.connectors.dynamodb import DynamoDbAttributeValue, DynamoDbSink, DynamoDbBeanElementConverter, \
    DynamoDbWriteRequest, DynamoDbWriteRequestType
from pyflink.testing.test_case_utils import PyFlinkUTTestCase
from pyflink.util.java_utils import get_field_value


class FlinkDynamoDbTest(PyFlinkUTTestCase):

    def test_dynamodb_sink(self):
        sink_properties = {
            'aws.region': 'eu-west-1',
            'aws.credentials.provider.basic.accesskeyid': 'aws_access_key_id',
            'aws.credentials.provider.basic.secretkey': 'aws_secret_access_key'
        }

        ds = self.env.from_collection([('ab', 1), ('bdc', 2), ('cfgs', 3), ('deeefg', 4)],
                                      type_info=Types.ROW([Types.STRING(), Types.INT()]))

        element_converter = DynamoDbBeanElementConverter(type_info=Types.ROW_NAMED(["stringVal", "numVal"],
                                                                                   [Types.STRING(), Types.INT()]))

        dynamodb_sink = DynamoDbSink.builder() \
            .set_element_converter(element_converter) \
            .set_dynamodb_client_properties(sink_properties) \
            .set_table_name('dynamodbTable-01') \
            .set_fail_on_error(False) \
            .set_max_batch_size(10) \
            .set_max_in_flight_requests(10) \
            .set_max_buffered_requests(10000) \
            .set_max_time_in_buffer_ms(5000) \
            .build()

        (ds.map(lambda element: {'stringVal': element[0], 'numVal': element[1]},
                output_type=Types.ROW_NAMED(["stringVal", "numVal"], [Types.STRING(), Types.INT()]))
         .sink_to(dynamodb_sink).name('DynamoDb Sink'))
        plan = eval(self.env.get_execution_plan())

        self.assertEqual('DynamoDb Sink: Writer', plan['nodes'][1]['type'])
        self.assertEqual(get_field_value(dynamodb_sink.get_java_function(), 'failOnError'),
                         False)
        self.assertEqual(
            get_field_value(dynamodb_sink.get_java_function(), 'tableName'),
            'dynamodbTable-01')
