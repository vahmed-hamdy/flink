package org.apache.flink.connectors.dummy.source;

public class CloudWatchSourceSplitState {
    private Long nextToken;

    public CloudWatchSourceSplitState(Long nextToken) {
        this.nextToken = nextToken;
    }

    public Long getNextToken() {
        return nextToken;
    }

    public void setNextToken(Long nextToken) {
        this.nextToken = nextToken;
    }
}
