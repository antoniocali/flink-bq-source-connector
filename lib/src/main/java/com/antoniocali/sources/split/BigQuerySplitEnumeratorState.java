package com.antoniocali.sources.split;

import lombok.Getter;

import java.io.Serializable;

@Getter
public class BigQuerySplitEnumeratorState implements Serializable {
    private final BigQuerySourceSplit currentSplit;
    private final boolean isAssigned;
    private final Long currentMaxTimestamp;


    public BigQuerySplitEnumeratorState(BigQuerySourceSplit currentSplit, boolean isAssigned,
            Long currentMaxTimestamp) {
        this.currentSplit = currentSplit;
        this.isAssigned = isAssigned;
        this.currentMaxTimestamp = currentMaxTimestamp;
    }
}
