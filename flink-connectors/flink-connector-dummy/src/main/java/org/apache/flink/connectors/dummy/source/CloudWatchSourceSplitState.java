package org.apache.flink.connectors.dummy.source;

import javax.annotation.Nullable;

import java.util.Objects;

public class CloudWatchSourceSplitState extends CloudWatchLogsSplit{
    @Nullable
    private String nextToken;

    @Nullable
    private Long currentTimeStamp;

    public CloudWatchSourceSplitState(CloudWatchLogsSplit split) {
        super(split.logGroup, split.logStream, split.startTimeStamp);
        this.currentTimeStamp = split.startTimeStamp;
    }


    public CloudWatchSourceSplitState(String logGroup, String logStream, @Nullable Long startTimeStamp, @Nullable Long currentTimeStamp, @Nullable String nextToken) {
        super(logGroup, logStream, startTimeStamp);
        this.nextToken = nextToken;
    }

    public void setNextToken(String nextToken) {
        this.nextToken = nextToken;
    }

    public String getNextToken(){
        return nextToken;
    }

    public Long getCurrentTimeStamp() {
        return currentTimeStamp;
    }
    public void setCurrentTimeStamp(Long currentTimeStamp){
        this.currentTimeStamp = currentTimeStamp;
    }

    public CloudWatchLogsSplit tpSplit() {
        return new CloudWatchLogsSplit(this.logGroup, this.logStream, this.currentTimeStamp);
    }
}
