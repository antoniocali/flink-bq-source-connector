package com.antoniocali;

import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import dojo.tech.dsp.flink.common.FlinkAvroUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.Test;
import tech.dojo.dsp.bq.converters.FieldValueListToRowDataConverters;

import java.sql.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ConverterTest {
    @Test
    public void testAddition() {
        var avroSchema = "{\"type\":\"record\",\"name\":\"dbt_full_test_2\",\"namespace\":\"data-mdm-dev.dsp_data_ingestion_data_streaming_platform\",\"doc\":\"Translated Avro Schema for data-mdm-dev.dsp_data_ingestion_data_streaming_platform.dbt_full_test_2\",\"fields\":[{\"name\":\"clearing_presentment_guid\",\"type\":[\"null\",\"string\"]},{\"name\":\"clearingRecordId\",\"type\":[\"null\",\"string\"]},{\"name\":\"schemeTransactionId\",\"type\":[\"null\",\"string\"]},{\"name\":\"authOperationId\",\"type\":[\"null\",\"string\"]},{\"name\":\"processingCode\",\"type\":[\"null\",\"string\"]},{\"name\":\"submissionFileId\",\"type\":[\"null\",\"string\"]},{\"name\":\"submissionTimestamp\",\"type\":[\"null\",\"string\"]},{\"name\":\"transaction_id\",\"type\":[\"null\",\"string\"]},{\"name\":\"dbt_updated_at\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}]},{\"name\":\"batch_number\",\"type\":[\"null\",\"long\"]},{\"name\":\"identifier\",\"type\":[\"null\",\"string\"]},{\"name\":\"interfaceName\",\"type\":[\"null\",\"string\"]},{\"name\":\"name\",\"type\":[\"null\",\"string\"]},{\"name\":\"datetime\",\"type\":[\"null\",\"string\"]}]}";
        RowType rowType = FlinkAvroUtils.AvroSchemaToRowType(avroSchema);
        FieldValueListToRowDataConverters.FieldValueListToRowDataConverter fieldValueListToRowDataConverter = FieldValueListToRowDataConverters.createRowConverter(
                rowType);
        var internalRecord = FieldValueList.of(List.of(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "f0"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "f1"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "f2"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "f3"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "f4"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "f5"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "f6"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "f7"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, 8),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "9"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "f10"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "f11"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "f12"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "f13")));
        var result = (GenericRowData) fieldValueListToRowDataConverter.convert(
                FieldValueList.of(List.of(FieldValue.of(FieldValue.Attribute.RECORD, internalRecord))));

        assertEquals(result.getArity(), 1);
        assertEquals(result.getRowKind(), RowKind.INSERT);
        var internalResult = result.getRow(0, 14);
        assertEquals(internalResult.getString(0).toString(), "f0");
    }
}
