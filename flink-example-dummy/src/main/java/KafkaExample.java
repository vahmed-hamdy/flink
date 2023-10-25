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


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Objects;
import java.util.Properties;

public class KafkaExample {
    public static void main(String []args) throws Exception {
        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // enable idempotence
        properties.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-tx-id");
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        String val = "";
        producer.initTransactions();
        producer.beginTransaction();
        do {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(System.in));

            val = reader.readLine();
            if(val.equals("COMMIT")) {
                producer.commitTransaction();
                break;
            }
            if(val.equals("ABORT")) {
                producer.abortTransaction();
                break;
            }

            // create a producer record
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>("quickstart-events", val);
            producer.send(producerRecord);
            producer.flush();
        }while (!Objects.equals(val, "STOP"));
        producer.close();
    }
}
