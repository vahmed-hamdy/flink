package org.apache.flink.connectors.dummy.source.enumerator;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class CloudWatchLogsEnumStateSerializer implements SimpleVersionedSerializer<CloudWatchLogsEnumState> {
    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(CloudWatchLogsEnumState obj) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(obj.getAssignedSplits().length);
            for(CloudWatchLogsEnumState.AssignedSplitState assignedSplitState: obj.getAssignedSplits()) {
                out.writeInt(assignedSplitState.getReader());
                out.writeUTF(assignedSplitState.getSplitId());
            }
            out.writeInt(obj.getFinishesSplits().length);
            for(String finishedSplit: obj.getFinishesSplits()) {
                out.writeUTF(finishedSplit);
            }
            return baos.toByteArray();
        }
    }

    @Override
    public CloudWatchLogsEnumState deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             DataInputStream in = new DataInputStream(bais)) {

            int assignesStateLen = in.readInt();

            CloudWatchLogsEnumState.AssignedSplitState []assignedSplitStates = new CloudWatchLogsEnumState.AssignedSplitState[assignesStateLen];
            for(int i=0;i<assignesStateLen;i++){
                assignedSplitStates[i] = new CloudWatchLogsEnumState.AssignedSplitState(in.readInt(), in.readUTF());
            }
            int finshedSplitLen = in.readInt();
            String[] finishedSplits = new String[finshedSplitLen];
            for(int i=0;i<finshedSplitLen;i++){
                finishedSplits[i] = in.readUTF();
            }
            return new CloudWatchLogsEnumState(assignedSplitStates, finishedSplits);
        }
    }
}
