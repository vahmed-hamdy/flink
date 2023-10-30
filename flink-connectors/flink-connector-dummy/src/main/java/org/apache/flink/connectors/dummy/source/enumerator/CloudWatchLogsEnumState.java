package org.apache.flink.connectors.dummy.source.enumerator;

public class CloudWatchLogsEnumState {
    private final AssignedSplitState[] assignedSplits;
    private final String[] finishesSplits;

    public static CloudWatchLogsEnumState emptyState() {
        return new CloudWatchLogsEnumState(new AssignedSplitState[0], new String[0]);
    }

    public CloudWatchLogsEnumState(AssignedSplitState[] assignedSplits, String[] finishesSplits) {
        this.assignedSplits = assignedSplits;
        this.finishesSplits = finishesSplits;
    }

    public AssignedSplitState[] getAssignedSplits() {
        return assignedSplits;
    }

    public String[] getFinishesSplits() {
        return finishesSplits;
    }

    public static class AssignedSplitState {
        private final String splitId;
        private final Integer reader;

        public AssignedSplitState(Integer reader, String splitId) {
            this.splitId = splitId;
            this.reader = reader;
        }

        public Integer getReader() {
            return reader;
        }

        public String getSplitId() {
            return splitId;
        }
    }
}
