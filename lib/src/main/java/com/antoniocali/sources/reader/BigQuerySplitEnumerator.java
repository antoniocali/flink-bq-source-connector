package com.antoniocali.sources.reader;

import com.antoniocali.bq.BigQueryReadOptions;
import com.antoniocali.sources.split.BigQuerySourceSplit;
import com.antoniocali.sources.split.BigQuerySplitEnumeratorState;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.antoniocali.bq.BigQueryClient;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;

public class BigQuerySplitEnumerator implements SplitEnumerator<BigQuerySourceSplit, BigQuerySplitEnumeratorState> {
    private final long DISCOVER_INTERVAL = 60_000L;
    private final long INITIAL_DELAY = 0L;
    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySplitEnumerator.class);
    protected Long maxCurrentTimetstamp;
    private final SplitEnumeratorContext<BigQuerySourceSplit> enumContext;
    private BigQuerySourceSplit currentSplit;
    private boolean isAssigned;
    private final BigQueryReadOptions readOptions;
    private final TreeSet<Integer> readersAwaitingSplit;

    public BigQuerySplitEnumerator(SplitEnumeratorContext<BigQuerySourceSplit> enumContext,
            BigQueryReadOptions readOptions, BigQuerySplitEnumeratorState enumeratorState) {
        this.enumContext = enumContext;

        this.maxCurrentTimetstamp = enumeratorState == null ? 0L : enumeratorState.getCurrentMaxTimestamp();
        this.readOptions = readOptions;
        this.currentSplit = enumeratorState == null ? new BigQuerySourceSplit("0L", 0L,
                Long.MAX_VALUE) : enumeratorState.getCurrentSplit();
        this.readersAwaitingSplit = new TreeSet<>();
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        SplitEnumerator.super.notifyCheckpointAborted(checkpointId);
    }

    @Override
    public void start() {
        LOG.info("Starting BigQuery split enumerator");
        this.scheduleNextSplit();
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!enumContext.registeredReaders().containsKey(subtaskId)) {
            // reader failed between sending the request and now. skip this request.
            return;
        }
        readersAwaitingSplit.add(subtaskId);
    }

    @Override
    public void addSplitsBack(List<BigQuerySourceSplit> list, int subTaskId) {
        if (!list.isEmpty()) {
            this.currentSplit = list.remove(0);
        }
        this.isAssigned = false;
    }

    @Override
    public void addReader(int i) {

    }

    @Override
    public BigQuerySplitEnumeratorState snapshotState(long checkpointId) throws Exception {
        return new BigQuerySplitEnumeratorState(this.currentSplit, this.isAssigned, this.maxCurrentTimetstamp);
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        SplitEnumerator.super.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        SplitEnumerator.super.handleSourceEvent(subtaskId, sourceEvent);
    }

    Optional<BigQuerySourceSplit> discoverNewSplit() {
        try {

            BigQuery bigquery = BigQueryClient.builder().setReadOptions(this.readOptions).build().getBigQuery();
            var tableName = readOptions.getFullTableName(true);
            var query = "SELECT MAX(" + readOptions.getColumnFetcher() + ") as max_timestamp FROM `" + tableName + "`";
            LOG.info("Discovering new split - Query: {}", query);
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).setUseLegacySql(false).build();
            TableResult result = bigquery.query(queryConfig);
            var maxColumnFetcher = result.iterateAll().iterator().next().get("max_timestamp");
            var maxTimestamp = convertFieldValueToEpochTime(maxColumnFetcher);
            if (this.maxCurrentTimetstamp == 0L) {
                LOG.info("Initial Load");
                return Optional.of(new BigQuerySourceSplit("InitialLoad", 0L, maxTimestamp));
            }
            if (maxTimestamp > maxCurrentTimetstamp) {
                LOG.info("Found a new split with new timestamp: {}", maxTimestamp);
                return Optional.of(
                        new BigQuerySourceSplit(Long.toString(maxTimestamp), maxCurrentTimetstamp, maxTimestamp));
            } else {
                LOG.info("No new split found");
                return Optional.empty();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * Function that handles discovery of a new Split Result
     *
     * @param newSplit It's an Optional because it's need to be combined with `DiscoverNewSplit`.
     * @param t
     */
    void handleDiscoverNewSplitResult(Optional<BigQuerySourceSplit> newSplit, Throwable t) {
        if (t != null) {
            throw new RuntimeException(t);
        }
        if (newSplit.isEmpty()) {
            // No new split
            return;
        }
        this.currentSplit = newSplit.get();
        this.maxCurrentTimetstamp = this.currentSplit.getMaxTimestamp();
        this.assignSplit();

    }

    void assignSplit() {
        final Iterator<Integer> awaitingReader = readersAwaitingSplit.iterator();
        LOG.info("Assigning split to readers");
        while (awaitingReader.hasNext()) {
            int nextAwaiting = awaitingReader.next();
            // if the reader that requested another split has failed in the meantime, remove
            // it from the list of waiting readers
            if (!enumContext.registeredReaders().containsKey(nextAwaiting)) {
                awaitingReader.remove();
                continue;
            }
            Optional<BigQuerySourceSplit> split = Optional.of(currentSplit);
            final BigQuerySourceSplit bqSplit = split.get();
            enumContext.assignSplit(bqSplit, nextAwaiting);
            this.isAssigned = true;
            awaitingReader.remove();
        }
    }


    private void scheduleNextSplit() {
        LOG.info("Scheduling Discovery next split");
        this.enumContext.callAsync(this::discoverNewSplit, this::handleDiscoverNewSplitResult, INITIAL_DELAY,
                DISCOVER_INTERVAL);
    }

    public long convertFieldValueToEpochTime(FieldValue fieldValue) {
        if (fieldValue.isNull()) {
            throw new IllegalArgumentException("FieldValue is null");
        }

        try {
            return fieldValue.getTimestampValue();
        } catch (NumberFormatException e) {
            LocalDate date = LocalDate.parse(fieldValue.getStringValue());
            return date.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        }
    }
}
