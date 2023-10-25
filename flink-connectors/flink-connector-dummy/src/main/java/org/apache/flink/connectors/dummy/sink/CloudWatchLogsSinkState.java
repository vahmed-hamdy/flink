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

package org.apache.flink.connectors.dummy.sink;

import org.apache.commons.lang3.Validate;

import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class CloudWatchLogsSinkState implements Serializable {
    String streamName;
    List<InputLogEvent> streamContent;
    public CloudWatchLogsSinkState(String streamName, List<InputLogEvent> streamContent) {
        this.streamName = streamName;
        this.streamContent = streamContent;
    }

    public static class CloudWatchLogsSinkStateSerializer implements SimpleVersionedSerializer<CloudWatchLogsSinkState> {


        private static final int MAGIC_NUMBER = 0x102901;

        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(CloudWatchLogsSinkState obj) throws IOException {
            DataOutputSerializer out = new DataOutputSerializer(256);
            out.writeInt(MAGIC_NUMBER);
            out.writeInt(obj.streamName.length());
            out.write(obj.streamName.getBytes(StandardCharsets.UTF_8));
            SimpleVersionedSerialization.writeVersionAndSerializeList(new SimpleVersionedSerializer<InputLogEvent>() {


                @Override
                public int getVersion() {
                    return 1;
                }

                @Override
                public byte[] serialize(InputLogEvent obj) throws IOException {
                    DataOutputSerializer out = new DataOutputSerializer(256);

                    out.writeLong(obj.timestamp());
                    out.writeInt(obj.message().length());
                    out.write(obj.message().getBytes(StandardCharsets.UTF_8));
                    return out.getCopyOfBuffer();
                }

                @Override
                public InputLogEvent deserialize(
                        int version,
                        byte[] serialized) throws IOException {
                    return null;
                }
            }, obj.streamContent, out);
            return out.getCopyOfBuffer();
        }

        @Override
        public CloudWatchLogsSinkState deserialize(
                int version,
                byte[] serialized) throws IOException {
            Validate.isTrue(version == 1);
            final DataInputDeserializer in = new DataInputDeserializer(serialized);
            Validate.isTrue(in.readInt() == MAGIC_NUMBER);
            int streamNameLen = in.readInt();
            byte []streamName = new byte[streamNameLen];
            int readBytes  = in.read(streamName, 0, streamNameLen);
            List<InputLogEvent> elements = SimpleVersionedSerialization.readVersionAndDeserializeList(
                    new SimpleVersionedSerializer<InputLogEvent>() {
                        @Override
                        public int getVersion() {
                            return 1;
                        }

                        @Override
                        public byte[] serialize(InputLogEvent obj) throws IOException {
                            return new byte[0];
                        }

                        @Override
                        public InputLogEvent deserialize(
                                int version,
                                byte[] serialized) throws IOException {
                            final DataInputDeserializer in = new DataInputDeserializer(serialized);
                            Long ts = in.readLong();
                            int len = in.readInt();
                            byte []message = new byte[len];
                            String messageStr = new String(message, StandardCharsets.UTF_8);
                            return InputLogEvent.builder().timestamp(ts).message(messageStr).build();
                        }
                    }, in);
            return new CloudWatchLogsSinkState(new String(streamName, StandardCharsets.UTF_8), elements);
        }
    }
}
