package com.antoniocali.sources.emitter;

import com.antoniocali.bq.BigQueryReadOptions;
import com.antoniocali.sources.split.BigQuerySourceSplitState;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import java.io.Serializable;

public class BigQueryRecordEmitter implements RecordEmitter<RowData, RowData, BigQuerySourceSplitState>, Serializable {

    private final BigQueryReadOptions readOptions;
    public BigQueryRecordEmitter(BigQueryReadOptions readOptions) {
        this.readOptions = readOptions;
    }

    @Override
    public void emitRecord(RowData rowData, SourceOutput<RowData> sourceOutput,
            BigQuerySourceSplitState bigQuerySourceSplitState) throws Exception {

        var columnName = readOptions.getColumnFetcher();
        var type = readOptions.getColumnFetcherTimeUnit();
        sourceOutput.collect(rowData);
    }


}
