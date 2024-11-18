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

import java.io.Closeable;
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

    /**
     * Closes this resource, relinquishing any underlying resources.
     * This method is invoked automatically on objects managed by the
     * {@code try}-with-resources statement.
     *
     * <p>While this interface method is declared to throw {@code
     * Exception}, implementers are <em>strongly</em> encouraged to
     * declare concrete implementations of the {@code close} method to
     * throw more specific exceptions, or to throw no exception at all
     * if the close operation cannot fail.
     *
     * <p> Cases where the close operation may fail require careful
     * attention by implementers. It is strongly advised to relinquish
     * the underlying resources and to internally <em>mark</em> the
     * resource as closed, prior to throwing the exception. The {@code
     * close} method is unlikely to be invoked more than once and so
     * this ensures that the resources are released in a timely manner.
     * Furthermore it reduces problems that could arise when the resource
     * wraps, or is wrapped, by another resource.
     *
     * <p><em>Implementers of this interface are also strongly advised
     * to not have the {@code close} method throw {@link
     * InterruptedException}.</em>
     * <p>
     * This exception interacts with a thread's interrupted status,
     * and runtime misbehavior is likely to occur if an {@code
     * InterruptedException} is {@linkplain Throwable#addSuppressed
     * suppressed}.
     * <p>
     * More generally, if it would cause problems for an
     * exception to be suppressed, the {@code AutoCloseable.close}
     * method should not throw it.
     *
     * <p>Note that unlike the {@link Closeable#close close}
     * method of {@link Closeable}, this {@code close} method
     * is <em>not</em> required to be idempotent.  In other words,
     * calling this {@code close} method more than once may have some
     * visible side effect, unlike {@code Closeable.close} which is
     * required to have no effect if called more than once.
     * <p>
     * However, implementers of this interface are strongly encouraged
     * to make their {@code close} methods idempotent.
     *
     * @throws Exception if this resource cannot be closed
     */
    @Override
    public void close() throws Exception {
        if (!closed) {
            closed = true;
            currentTimestamp = 0L;
        }
    }
}
