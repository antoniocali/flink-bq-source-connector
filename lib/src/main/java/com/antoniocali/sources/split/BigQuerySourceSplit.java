package com.antoniocali.sources.split;

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;

public class BigQuerySourceSplit implements SourceSplit, Serializable {
    private final String splitName;
    private final Long minTimestamp;
    private final Long maxTimestamp;

    public BigQuerySourceSplit(String splitName, Long minTimestamp, Long maxTimestamp) {
        this.splitName = splitName;
        this.maxTimestamp = maxTimestamp;
        this.minTimestamp = minTimestamp;
    }

    @Override
    public String splitId() {
        return splitName;
    }

    public String getSplitName() {
        return splitName;
    }

    public Long getMinTimestamp() {
        return minTimestamp;
    }

    public Long getMaxTimestamp() {
        return maxTimestamp;
    }

    @Override
    public String toString() {
        return "BigQuerySourceSplit{" + "splitName='" + splitName + '\'' + ", minTimestamp=" + minTimestamp + ", maxTimestamp=" + maxTimestamp + '}';
    }
}
