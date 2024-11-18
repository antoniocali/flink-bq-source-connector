package com.antoniocali.bq;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

import java.io.Serializable;

@Getter
@Builder(setterPrefix = "set")
public class BigQueryReadOptions implements Serializable  {

    @NonNull
    private final String projectId;

    @NonNull
    private final String dataset;

    @NonNull
    private final String table;

    @NonNull
    private final String columnFetcher;

    @NonNull
    private final TimeUnit columnFetcherTimeUnit;

    public String getFullTableName(boolean withProject) {
        if (withProject) {
            return String.format("%s.%s.%s", projectId, dataset, table);
        }
        return String.format("%s.%s", dataset, table);
    }

    public enum TimeUnit implements Serializable {
        DAY, SECONDS, MILLIS, MICROS
    }


}
