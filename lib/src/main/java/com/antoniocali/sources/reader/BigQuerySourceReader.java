package com.antoniocali.sources.reader;

import com.antoniocali.sources.split.BigQuerySourceSplitState;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.antoniocali.sources.split.BigQuerySourceSplit;

import java.util.function.Supplier;
import java.util.Map;

public class BigQuerySourceReader
        extends SingleThreadMultiplexSourceReaderBase<RowData, RowData, BigQuerySourceSplit, BigQuerySourceSplitState> {
    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySourceReader.class);

    public BigQuerySourceReader(FutureCompletingBlockingQueue<RecordsWithSplitIds<RowData>> elementsQueue,
            Supplier<SplitReader<RowData, BigQuerySourceSplit>> splitFetcherManager,
            RecordEmitter<RowData, RowData, BigQuerySourceSplitState> recordEmitter, Configuration config,
            SourceReaderContext context) {
        super(elementsQueue, splitFetcherManager, recordEmitter, config, context);
    }

    @Override
    public void start() {
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected void onSplitFinished(Map<String, BigQuerySourceSplitState> finishedSplitIds) {
        for (BigQuerySourceSplitState splitState : finishedSplitIds.values()) {
            BigQuerySourceSplit sourceSplit = splitState.toBigQuerySourceSplit();
            LOG.info("Read for split {} is completed.", sourceSplit.splitId());
        }
        context.sendSplitRequest();
    }

    @Override
    protected BigQuerySourceSplitState initializedState(BigQuerySourceSplit split) {
        return new BigQuerySourceSplitState(split.splitId(), split.getMinTimestamp(), split.getMaxTimestamp());
    }

    @Override
    protected BigQuerySourceSplit toSplitType(String splitId, BigQuerySourceSplitState sst) {
        return new BigQuerySourceSplit(splitId, sst.getMinCurrentTimestamp(), sst.getMaxCurrentTimestamp());
    }
}
