package org.apache.flink.connectors.dummy.source;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.OutputLogEvent;

import java.util.function.Function;

public class CloudWatchLogsRecordEmitter<T> implements RecordEmitter<OutputLogEvent, T, CloudWatchSourceSplitState> {
    private final Function<OutputLogEvent, T> converter;
    public CloudWatchLogsRecordEmitter(final Function<OutputLogEvent, T> converter) {
        this.converter = converter;
    }
    @Override
    public void emitRecord(
            OutputLogEvent element,
            SourceOutput<T> output,
            CloudWatchSourceSplitState splitState) throws Exception {
        output.collect(this.converter.apply(element));
        // TODO replace with Token and use response
        splitState.setNextToken(element.timestamp());

    }
}
