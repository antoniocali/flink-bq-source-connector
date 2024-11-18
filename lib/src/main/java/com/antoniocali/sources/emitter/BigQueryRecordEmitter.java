package com.antoniocali.sources.emitter;

import com.antoniocali.sources.split.BigQuerySourceSplitState;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.table.data.RowData;

import java.io.Serializable;

public class BigQueryRecordEmitter implements RecordEmitter<RowData, RowData, BigQuerySourceSplitState>, Serializable {

    @Override
    public void emitRecord(RowData rowData, SourceOutput<RowData> sourceOutput,
            BigQuerySourceSplitState bigQuerySourceSplitState) throws Exception {

        sourceOutput.collect(rowData);
    }

}
