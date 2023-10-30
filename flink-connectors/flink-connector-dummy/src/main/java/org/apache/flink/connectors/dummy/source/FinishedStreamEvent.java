package org.apache.flink.connectors.dummy.source;

import org.apache.flink.api.connector.source.SourceEvent;

import java.util.List;

public class FinishedStreamEvent implements SourceEvent {
    private final List<String> logStreams;

    public FinishedStreamEvent(List<String> logStreams) {
        this.logStreams = logStreams;
    }

    public List<String> getLogStream() {
        return logStreams;
    }
}
