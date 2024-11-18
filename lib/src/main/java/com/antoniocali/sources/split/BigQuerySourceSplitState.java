package com.antoniocali.sources.split;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class BigQuerySourceSplitState implements Serializable {
    private String splitId;
    private Long minCurrentTimestamp;
    private Long maxCurrentTimestamp;

    public BigQuerySourceSplitState(@NonNull String splitId, @NonNull Long minCurrentTimestamp,
            @NonNull Long currentTimestamp) {
        this.splitId = splitId;
        this.minCurrentTimestamp = minCurrentTimestamp;
        this.maxCurrentTimestamp = currentTimestamp;
    }

    public BigQuerySourceSplit toBigQuerySourceSplit() {
        return new BigQuerySourceSplit(splitId, minCurrentTimestamp, maxCurrentTimestamp);
    }
}
