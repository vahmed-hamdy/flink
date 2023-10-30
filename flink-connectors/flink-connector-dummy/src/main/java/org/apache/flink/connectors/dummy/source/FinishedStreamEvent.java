package org.apache.flink.connectors.dummy.source;

import org.apache.flink.api.connector.source.SourceEvent;

public class FinishedStreamEvent implements SourceEvent {
    private final String logStream;

    public FinishedStreamEvent(String logStream) {
        this.logStream = logStream;
    }

    public String getLogStream() {
        return logStream;
    }
}
