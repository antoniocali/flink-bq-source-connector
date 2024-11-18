package com.antoniocali.bq;

import com.antoniocali.sources.split.BigQuerySourceSplit;

public class BigQueryUtils {

    public static String getSqlFromSplit(BigQuerySourceSplit bigQuerySourceSplit,
            BigQueryReadOptions bigQueryReadOptions) {
        String tableName = bigQueryReadOptions.getFullTableName(true);
        String columnName = bigQueryReadOptions.getColumnFetcher();
        String epochConverterFunction = epochConverter(bigQueryReadOptions.getColumnFetcherTimeUnit());
        String columnNameConverted = epochConverterFunction + "(" + columnName + ")";
        return "SELECT * " + "FROM " + tableName + " WHERE " + columnNameConverted + " BETWEEN " + bigQuerySourceSplit.getMinTimestamp() + " AND " + bigQuerySourceSplit.getMaxTimestamp();
    }

    private static String epochConverter(BigQueryReadOptions.TimeUnit timeUnit) {
        switch (timeUnit) {
            case DAY:
                return "UNIX_DATE";
            case SECONDS:
                return "TIMESTAMP_SECONDS";
            case MILLIS:
                return "TIMESTAMP_MILLIS";
            case MICROS:
                return "TIMESTAMP_MICROS";
            default:
                throw new RuntimeException();
        }
    }

}
