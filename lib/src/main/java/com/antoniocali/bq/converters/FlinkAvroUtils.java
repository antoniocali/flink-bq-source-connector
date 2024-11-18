package com.antoniocali.bq.converters;

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;

// This class provides utility methods for working between Avro and Flink table api datatypes.
public class FlinkAvroUtils {
    public static RowType AvroSchemaToRowType(String schema) {
        DataType dataType = AvroSchemaConverter.convertToDataType(schema);
        return RowType.of(dataType.getLogicalType());
    }

    public static AvroToRowDataConverters.AvroToRowDataConverter NewAvroToRowDataConverter(RowType rowType) {
        return AvroToRowDataConverters.createRowConverter(rowType);
    }
}
