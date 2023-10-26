package org.apache.flink.connectors.dummy.source;

import org.apache.flink.api.connector.source.SourceSplit;

public class CloudWatchLogsSplit implements SourceSplit {
    @Override
    public String splitId() {
        return null;
    }
}
