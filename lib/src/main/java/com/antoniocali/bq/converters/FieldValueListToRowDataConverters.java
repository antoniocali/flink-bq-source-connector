package com.antoniocali.bq.converters;

import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import java.io.Serializable;
import java.time.*;
import java.util.*;
import java.util.logging.Logger;

@Internal
public class FieldValueListToRowDataConverters {
    public static Logger LOG = Logger.getLogger(FieldValueListToRowDataConverters.class.getName());

    @FunctionalInterface
    public interface FieldValueListToRowDataConverter extends Serializable {
        RowData convert(FieldValueList object);
    }

    @FunctionalInterface
    public interface FieldValueToRowFieldConverter extends Serializable {
        Object convert(FieldValue object);
    }


    public static FieldValueListToRowDataConverter createRowConverter(RowType rowType) {
        return createRowConverter(rowType, true);
    }

    public static FieldValueListToRowDataConverter createRowConverter(RowType rowType, boolean legacyTimestampMapping) {
        final int arity = rowType.getFieldCount();
        final List<RowType.RowField> rowFields = rowType.getFields();
        return fieldObject -> {
            GenericRowData row = new GenericRowData(arity);
            for (int i = 0; i < arity; ++i) {
                LogicalType logicalType = rowFields.get(i).getType();
                row.setField(i,
                        createNullableConverter(logicalType, legacyTimestampMapping).convert(fieldObject.get(i)));
            }
            return row;
        };
    }

    /**
     * Creates a runtime converter which is null safe.
     */
    private static FieldValueToRowFieldConverter createNullableConverter(LogicalType type,
            boolean legacyTimestampMapping) {
        final FieldValueToRowFieldConverter converterFieldValue = createConverter(type, legacyTimestampMapping);
        return fieldValue -> {
            if (fieldValue == null) {
                return null;
            }
            return converterFieldValue.convert(fieldValue);
        };
    }


    /**
     * Creates a runtime converter which assuming input object is not null.
     */
    private static FieldValueToRowFieldConverter createConverter(LogicalType type, boolean legacyTimestampMapping) {
        switch (type.getTypeRoot()) {
            case NULL:
                return fieldType -> null;
            case TINYINT:
            case SMALLINT:
            case INTEGER: // int
            case INTERVAL_YEAR_MONTH: // long
            case INTERVAL_DAY_TIME: // long
            case BIGINT: // long
                return fieldType -> fieldType.isNull() ? null : RawValueData.fromObject(fieldType.getLongValue());
            case FLOAT:
            case DOUBLE: // double
                return fieldType -> fieldType.isNull() ? null : RawValueData.fromObject(fieldType.getDoubleValue());// float
            case BOOLEAN: // boolean
                return fieldType -> fieldType.isNull() ? null : RawValueData.fromObject(fieldType.getBooleanValue());
            case DATE:
                return FieldValue::getValue;
            case TIME_WITHOUT_TIME_ZONE:
                return fieldType -> fieldType.isNull() ? null : TimestampData.fromEpochMillis(fieldType.getTimestampValue());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                int precision = LogicalTypeChecks.getPrecision(type);
                if (precision <= 3) {
                    return FieldValueListToRowDataConverters.epochConverter(false);
                } else if (precision <= 6) {
                    return FieldValueListToRowDataConverters.epochConverter(true);
                } else {
                    throw new UnsupportedOperationException("Unsupported type: " + type);
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (legacyTimestampMapping) {
                    throw new UnsupportedOperationException("Unsupported type: " + type);
                } else {
                    int localPrecision = LogicalTypeChecks.getPrecision(type);
                    if (localPrecision <= 3) {
                        return FieldValueListToRowDataConverters.epochConverter(false);
                    } else if (localPrecision <= 6) {
                        return FieldValueListToRowDataConverters.epochConverter(true);
                    } else {
                        throw new UnsupportedOperationException("Unsupported type: " + type);
                    }
                }
            case CHAR:
            case VARCHAR:
                return fieldType -> fieldType.isNull() ? null : StringData.fromString(fieldType.getStringValue());
            case BINARY:
            case VARBINARY:
                return fieldType -> fieldType.isNull() ? null : fieldType.getBytesValue();
            case DECIMAL:
                return fieldType -> fieldType.isNull() ? null : fieldType.getNumericValue();
            case ARRAY:
                return fieldType -> fieldType.isNull() ? null : fieldType.getRepeatedValue();
            case ROW:
            case MAP:
            case MULTISET:
                return fieldValue -> {
                    List<LogicalType> rowType = type.getChildren();
                    var myNewRowType = RowType.of(rowType.toArray(LogicalType[]::new));
                    var converter = createRowConverter(myNewRowType, legacyTimestampMapping);
                    return converter.convert(fieldValue.getRecordValue());
                };
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }


    private static FieldValueToRowFieldConverter epochConverter(boolean nanosPrecision) {
        return fieldValue -> {
            if (fieldValue.isNull()) return null;
            Instant instant = fieldValue.getTimestampInstant();
            long epochSecond = instant.getEpochSecond();
            int nanoAdjustment = nanosPrecision ? instant.getNano() : 0;
            return TimestampData.fromEpochMillis(epochSecond, nanoAdjustment);
        };

    }
}
