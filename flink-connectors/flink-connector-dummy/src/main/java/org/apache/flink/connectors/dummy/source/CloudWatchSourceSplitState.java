package org.apache.flink.connectors.dummy.source;

import javax.annotation.Nullable;

import java.util.Objects;

public class CloudWatchSourceSplitState extends CloudWatchLogsSplit{
    @Nullable
    private String nextToken;

    @Nullable
    private Long currentTimeStamp;


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

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), currentTimeStamp);
    }

    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof CloudWatchLogsSplit)){
            return false;
        }
        CloudWatchSourceSplitState other = (CloudWatchSourceSplitState) obj;
        return super.equals(obj)
                && Objects.equals(this.currentTimeStamp, other.currentTimeStamp)
                && Objects.equals(this.nextToken, other.nextToken);
    }
}
