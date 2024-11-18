package com.antoniocali.sources.reader;

import com.antoniocali.bq.BigQueryReadOptions;
import com.antoniocali.bq.BigQueryUtils;
import com.antoniocali.bq.converters.FlinkAvroUtils;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.flink.bigquery.common.utils.SchemaTransform;
import org.apache.avro.Schema;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.antoniocali.bq.BigQueryClient;
import com.antoniocali.bq.converters.FieldValueListToRowDataConverters;
import com.antoniocali.sources.split.BigQuerySourceSplit;

import java.io.IOException;
import java.util.*;

public class BigQuerySplitReader implements SplitReader<RowData, BigQuerySourceSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySplitReader.class);
    private final Queue<BigQuerySourceSplit> assignedSplits = new ArrayDeque<>();
    private final BigQueryReadOptions readOptions;
    private Long currentTimestamp = 0L;
    private Boolean closed = false;

    public BigQuerySplitReader(BigQueryReadOptions readOptions) {
        this.readOptions = readOptions;
    }


    // To check where to use
    Long currentTimestampToFetch(BigQuerySourceSplit split) {
        if (split.getMaxTimestamp() > 0) {
            currentTimestamp = split.getMaxTimestamp();
        }
        return currentTimestamp;
    }

    private Iterator<FieldValueList> retrieveSplitData(BigQuerySourceSplit bqSplit) throws IOException, InterruptedException {
        BigQueryClient bigQueryClient = BigQueryClient.builder().setReadOptions(readOptions).build();
        String sqlQuery = BigQueryUtils.getSqlFromSplit(bqSplit, readOptions);
        LOG.info("Retrieving Data for - {}", bqSplit);
        LOG.info("SQL query: {}", sqlQuery);

        BigQuery bigQuery = bigQueryClient.getBigQuery();
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sqlQuery).setUseLegacySql(false).build();
        TableResult tableResult = bigQuery.query(queryConfig);
        return tableResult.iterateAll().iterator();
    }

    @Override
    public RecordsWithSplitIds<RowData> fetch() throws IOException {
        if (closed) {
            throw new IllegalStateException("Can't fetch records from a closed split reader.");
        }
        RecordsBySplits.Builder<RowData> respBuilder = new RecordsBySplits.Builder<>();
        var currentSplit = Optional.ofNullable(assignedSplits.poll());
        if (currentSplit.isEmpty()) {
            LOG.info("current split is empty");
            return respBuilder.build();
        }
        BigQueryClient bigQueryClient = BigQueryClient.builder().setReadOptions(readOptions).build();
        TableSchema tableSchema = bigQueryClient.getTableSchema();
        Schema avroSchema = SchemaTransform.toGenericAvroSchema(readOptions.getFullTableName(true),
                tableSchema.getFields());
        RowType rowType = (RowType) FlinkAvroUtils.AvroSchemaToRowType(avroSchema.toString()).getTypeAt(0);
        FieldValueListToRowDataConverters.FieldValueListToRowDataConverter fieldValueListToRowDataConverter = FieldValueListToRowDataConverters.createRowConverter(
                rowType);
        var actualSplit = currentSplit.get();
        LOG.info("actual split is {}", actualSplit);
        var read = 0L;
        try {
            var records = retrieveSplitData(actualSplit);
            while (records.hasNext()) {
                var record = records.next();
                read++;
                var converted = fieldValueListToRowDataConverter.convert(record);
                respBuilder.add(actualSplit.splitId(), converted);
            }
            respBuilder.addFinishedSplit(actualSplit.splitId());
            currentTimestamp = actualSplit.getMaxTimestamp();
            LOG.info("Finish Reading {} - Total Records {}", actualSplit, read);
            return respBuilder.build();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void handleSplitsChanges(SplitsChange<BigQuerySourceSplit> splitsChanges) {
        LOG.debug("Handle split changes {}.", splitsChanges);
        assignedSplits.addAll(splitsChanges.splits());
    }

    @Override
    public void wakeUp() {

    }

    @Override
    public void pauseOrResumeSplits(Collection<BigQuerySourceSplit> splitsToPause,
            Collection<BigQuerySourceSplit> splitsToResume) {
        SplitReader.super.pauseOrResumeSplits(splitsToPause, splitsToResume);
    }


    @Override
    public void close() throws Exception {
        if (!closed) {
            closed = true;
            currentTimestamp = 0L;
        }
    }
}
