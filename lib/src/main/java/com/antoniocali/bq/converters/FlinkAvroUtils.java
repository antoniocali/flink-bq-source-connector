package com.antoniocali.bq.converters;

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;

public class FlinkAvroUtils {
    public static RowType AvroSchemaToRowType(String schema) {
        DataType dataType = AvroSchemaConverter.convertToDataType(schema);
        return RowType.of(dataType.getLogicalType());
    }
}
