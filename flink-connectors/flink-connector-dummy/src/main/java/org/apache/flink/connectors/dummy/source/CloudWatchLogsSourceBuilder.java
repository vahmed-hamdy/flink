package org.apache.flink.connectors.dummy.source;

import software.amazon.awssdk.services.cloudwatchlogs.model.OutputLogEvent;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public class CloudWatchLogsSourceBuilder<T> {
    private Function<OutputLogEvent, T> converter;

    private String logGroup;
    private String streamPrefixes;

    public CloudWatchLogsSourceBuilder<T> withLogGroup(final String logGroup) {
        this.logGroup = logGroup;
        return this;
    }


    public CloudWatchLogsSourceBuilder<T> withConverter(final Function<OutputLogEvent, T> converter) {
        this.converter = converter;
        return this;
    }

    public CloudWatchLogsSourceBuilder<T> withPrefixes(final String prefix) {
        this.streamPrefixes = prefix;
        return this;
    }

    CloudWatchLogsSource<T> buildSource() {
        return new CloudWatchLogsSource<>(converter, logGroup, streamPrefixes);
    }
}
