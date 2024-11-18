package com.antoniocali.sources;

import com.antoniocali.sources.split.BigQuerySourceSplit;
import com.antoniocali.sources.split.BigQuerySourceSplitSerializer;
import com.antoniocali.sources.split.BigQuerySplitEnumeratorState;
import com.antoniocali.sources.split.BigQuerySplitEnumeratorStateSerializer;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.antoniocali.bq.BigQueryReadOptions;
import com.antoniocali.sources.emitter.BigQueryRecordEmitter;
import com.antoniocali.sources.reader.BigQuerySourceReader;
import com.antoniocali.sources.reader.BigQuerySplitEnumerator;
import com.antoniocali.sources.reader.BigQuerySplitReader;

import java.util.function.Supplier;

public class BigQueryDataStreamSource implements Source<RowData, BigQuerySourceSplit, BigQuerySplitEnumeratorState> {
    private static final Logger LOG = LoggerFactory.getLogger(BigQueryDataStreamSource.class);

    final BigQueryReadOptions readOptions;

    public BigQueryDataStreamSource(BigQueryReadOptions readOptions) {
        this.readOptions = readOptions;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<BigQuerySourceSplit, BigQuerySplitEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<BigQuerySourceSplit> splitEnumeratorContext,
            BigQuerySplitEnumeratorState bigQuerySplitEnumeratorState) throws Exception {
        LOG.info("Restoring Enumerator with following State: {}", bigQuerySplitEnumeratorState);
        return new BigQuerySplitEnumerator(splitEnumeratorContext, readOptions, bigQuerySplitEnumeratorState);

    }

    @Override
    public SplitEnumerator<BigQuerySourceSplit, BigQuerySplitEnumeratorState> createEnumerator(
            SplitEnumeratorContext<BigQuerySourceSplit> splitEnumeratorContext) throws Exception {
        LOG.info("Creating new Enumerator");
        return new BigQuerySplitEnumerator(splitEnumeratorContext, readOptions, null);
    }


    @Override
    public SimpleVersionedSerializer<BigQuerySourceSplit> getSplitSerializer() {
        return BigQuerySourceSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<BigQuerySplitEnumeratorState> getEnumeratorCheckpointSerializer() {
        return BigQuerySplitEnumeratorStateSerializer.INSTANCE;
    }

    @Override
    public SourceReader<RowData, BigQuerySourceSplit> createReader(SourceReaderContext sourceReaderContext) throws
            Exception {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<RowData>> elementsQueue = new FutureCompletingBlockingQueue<>();
        BigQueryRecordEmitter recordEmitter = new BigQueryRecordEmitter();
        Supplier<SplitReader<RowData, BigQuerySourceSplit>> splitReaderSupplier = () -> new BigQuerySplitReader(
                readOptions);
        return new BigQuerySourceReader(elementsQueue, splitReaderSupplier, recordEmitter, new Configuration(),
                sourceReaderContext);
    }
}
