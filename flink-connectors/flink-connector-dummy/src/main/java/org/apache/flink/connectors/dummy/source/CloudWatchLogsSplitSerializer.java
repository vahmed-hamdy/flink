package org.apache.flink.connectors.dummy.source;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class CloudWatchLogsSplitSerializer implements SimpleVersionedSerializer<CloudWatchLogsSplit>{

    private final String logGroup;

    public CloudWatchLogsSplitSerializer(String logGroup) {
        this.logGroup = logGroup;
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(CloudWatchLogsSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(logGroup);
            out.writeUTF(split.splitId());
            out.writeBoolean(split.getStartTimeStamp() != null);
            if(split.getStartTimeStamp() != null)
               out.writeLong(split.getStartTimeStamp());
            out.flush();
            return baos.toByteArray();
        }

    }

    @Override
    public CloudWatchLogsSplit deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             DataInputStream in = new DataInputStream(bais)) {
            String logGroup = in.readUTF();
            String logStream = in.readUTF();
            boolean hasStart = in.readBoolean();
            Long start = hasStart ? in.readLong() : null;
            return new CloudWatchLogsSplit(logGroup, logStream, start);
        }
    }
}
