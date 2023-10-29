package org.apache.flink.connectors.dummy.source;

import org.apache.flink.api.connector.source.SourceSplit;

import javax.annotation.Nullable;

import java.util.Objects;


public class CloudWatchLogsSplit implements SourceSplit {

    protected final String logGroup;
    protected final String logStream;

    @Nullable
    protected Long startTimeStamp;

    public CloudWatchLogsSplit(String logGroup, String logStream) {
        this(logGroup, logStream, null);
    }

    public CloudWatchLogsSplit(String logGroup, String logStream, @Nullable Long startTimeStamp) {
        this.logGroup = logGroup;
        this.logStream = logStream;
        this.startTimeStamp = startTimeStamp;
    }
    @Override
    public String splitId() {
        return logStream;
    }

    public Long getStartTimeStamp() {
        return startTimeStamp;
    }
    public void setStartTimeStamp(Long startTimeStamp){
        this.startTimeStamp = startTimeStamp;
    }
    @Override
    public int hashCode() {
        return Objects.hash(logGroup, logStream, startTimeStamp);
    }

    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof CloudWatchLogsSplit)){
            return false;
        }
        CloudWatchLogsSplit other = (CloudWatchLogsSplit) obj;
        return logGroup.equals(other.logGroup) && logStream.equals(other.logStream)
                && Objects.equals(this.startTimeStamp, other.startTimeStamp);
    }
}
