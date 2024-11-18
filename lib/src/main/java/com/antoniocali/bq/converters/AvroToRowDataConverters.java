package com.antoniocali.bq.converters;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.formats.avro.typeutils.AvroSchemaConverter.extractValueTypeToAvroMap;

/** Tool class used to convert from Avro {@link GenericRecord} to {@link RowData}. * */
// DSP: We've had to fork this one to include the Avro Timestamp micro support
// I've removed the Joda converter for simplicity, we most likely won't use it
@Internal
public class AvroToRowDataConverters {

    /**
     * Runtime converter that converts Avro data structures into objects of Flink Table & SQL
     * internal data structures.
     */
    @FunctionalInterface
    public interface AvroToRowDataConverter extends Serializable {
        Object convert(Object object);
    }

    // -------------------------------------------------------------------------------------
    // Runtime Converters
    // -------------------------------------------------------------------------------------

    public static AvroToRowDataConverters.AvroToRowDataConverter createRowConverter(RowType rowType) {
        return createRowConverter(rowType, true);
    }

    public static AvroToRowDataConverters.AvroToRowDataConverter createRowConverter(
            RowType rowType, boolean legacyTimestampMapping) {
        final AvroToRowDataConverters.AvroToRowDataConverter[] fieldConverters =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .map(type -> createNullableConverter(type, legacyTimestampMapping))
                        .toArray(AvroToRowDataConverters.AvroToRowDataConverter[]::new);
        final int arity = rowType.getFieldCount();

        return avroObject -> {
            IndexedRecord record = (IndexedRecord) avroObject;
            GenericRowData row = new GenericRowData(arity);
            for (int i = 0; i < arity; ++i) {
                // avro always deserialize successfully even though the type isn't matched
                // so no need to throw exception about which field can't be deserialized
                row.setField(i, fieldConverters[i].convert(record.get(i)));
            }
            return row;
        };
    }

    /** Creates a runtime converter which is null safe. */
    private static AvroToRowDataConverters.AvroToRowDataConverter createNullableConverter(
            LogicalType type, boolean legacyTimestampMapping) {
        final AvroToRowDataConverters.AvroToRowDataConverter converter = createConverter(type, legacyTimestampMapping);
        return avroObject -> {
            if (avroObject == null) {
                return null;
            }
            return converter.convert(avroObject);
        };
    }

    /** Creates a runtime converter which assuming input object is not null. */
    private static AvroToRowDataConverters.AvroToRowDataConverter createConverter(
            LogicalType type, boolean legacyTimestampMapping) {
        switch (type.getTypeRoot()) {
            case NULL:
                return avroObject -> null;
            case TINYINT:
                return avroObject -> ((Integer) avroObject).byteValue();
            case SMALLINT:
                return avroObject -> ((Integer) avroObject).shortValue();
            case BOOLEAN: // boolean
            case INTEGER: // int
            case INTERVAL_YEAR_MONTH: // long
            case BIGINT: // long
            case INTERVAL_DAY_TIME: // long
            case FLOAT: // float
            case DOUBLE: // double
                return avroObject -> avroObject;
            case DATE:
                return AvroToRowDataConverters::convertToDate;
            case TIME_WITHOUT_TIME_ZONE:
                return AvroToRowDataConverters::convertToTime;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                int precision = LogicalTypeChecks.getPrecision(type);
                if (precision <= 3) {
                    return AvroToRowDataConverters::convertToTimestampMillis;
                } else if (precision <= 6) {
                    return AvroToRowDataConverters::convertToTimestampMicros;
                } else {
                    throw new UnsupportedOperationException("Unsupported type: " + type);
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (legacyTimestampMapping) {
                    throw new UnsupportedOperationException("Unsupported type: " + type);
                } else {
                    int localPrecision = LogicalTypeChecks.getPrecision(type);
                    if (localPrecision <= 3) {
                        return AvroToRowDataConverters::convertToTimestampMillis;
                    } else if (localPrecision <= 6) {
                        return AvroToRowDataConverters::convertToTimestampMicros;
                    } else {
                        throw new UnsupportedOperationException("Unsupported type: " + type);
                    }
                }
            case CHAR:
            case VARCHAR:
                return avroObject -> StringData.fromString(avroObject.toString());
            case BINARY:
            case VARBINARY:
                return AvroToRowDataConverters::convertToBytes;
            case DECIMAL:
                return createDecimalConverter((DecimalType) type);
            case ARRAY:
                return createArrayConverter((ArrayType) type, legacyTimestampMapping);
            case ROW:
                return createRowConverter((RowType) type);
            case MAP:
            case MULTISET:
                return createMapConverter(type, legacyTimestampMapping);
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private static AvroToRowDataConverters.AvroToRowDataConverter createDecimalConverter(DecimalType decimalType) {
        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();
        return avroObject -> {
            final byte[] bytes;
            if (avroObject instanceof GenericFixed) {
                bytes = ((GenericFixed) avroObject).bytes();
            } else if (avroObject instanceof ByteBuffer) {
                ByteBuffer byteBuffer = (ByteBuffer) avroObject;
                bytes = new byte[byteBuffer.remaining()];
                byteBuffer.get(bytes);
            } else {
                bytes = (byte[]) avroObject;
            }
            return DecimalData.fromUnscaledBytes(bytes, precision, scale);
        };
    }

    private static AvroToRowDataConverters.AvroToRowDataConverter createArrayConverter(
            ArrayType arrayType, boolean legacyTimestampMapping) {
        final AvroToRowDataConverters.AvroToRowDataConverter elementConverter =
                createNullableConverter(arrayType.getElementType(), legacyTimestampMapping);
        final Class<?> elementClass =
                LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());

        return avroObject -> {
            final List<?> list = (List<?>) avroObject;
            final int length = list.size();
            final Object[] array = (Object[]) Array.newInstance(elementClass, length);
            for (int i = 0; i < length; ++i) {
                array[i] = elementConverter.convert(list.get(i));
            }
            return new GenericArrayData(array);
        };
    }

    private static AvroToRowDataConverters.AvroToRowDataConverter createMapConverter(
            LogicalType type, boolean legacyTimestampMapping) {
        final AvroToRowDataConverters.AvroToRowDataConverter keyConverter =
                createConverter(DataTypes.STRING().getLogicalType(), legacyTimestampMapping);
        final AvroToRowDataConverters.AvroToRowDataConverter valueConverter =
                createNullableConverter(extractValueTypeToAvroMap(type), legacyTimestampMapping);

        return avroObject -> {
            final Map<?, ?> map = (Map<?, ?>) avroObject;
            Map<Object, Object> result = new HashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                Object key = keyConverter.convert(entry.getKey());
                Object value = valueConverter.convert(entry.getValue());
                result.put(key, value);
            }
            return new GenericMapData(result);
        };
    }

    private static TimestampData convertToTimestampMicros(Object object) {
        final long micros;
        if (object instanceof Long) {
            micros = (Long) object;
        } else if (object instanceof Instant) {
            micros =
                    ((Instant) object).getEpochSecond() * 1_000_000
                    + ((Instant) object).get(ChronoField.MICRO_OF_SECOND);
        } else {
            throw new IllegalArgumentException(
                    "Unexpected object type for TIMESTAMP logical type. Received: " + object);
        }
        long epochSeconds = micros / (1_000_000L);
        long nanoAdjustment = (micros % (1_000_000L)) * 1_000L;

        return TimestampData.fromInstant(Instant.ofEpochSecond(epochSeconds, nanoAdjustment));
    }

    private static TimestampData convertToTimestampMillis(Object object) {
        final long millis;
        if (object instanceof Long) {
            millis = (Long) object;
        } else if (object instanceof Instant) {
            millis = ((Instant) object).toEpochMilli();
        } else if (object instanceof LocalDateTime) {
            return TimestampData.fromLocalDateTime((LocalDateTime) object);
        } else {
            throw new IllegalArgumentException(
                    "Unexpected object type for TIMESTAMP logical type. Received: " + object);
        }
        return TimestampData.fromEpochMillis(millis);
    }

    private static int convertToDate(Object object) {
        if (object instanceof Integer) {
            return (Integer) object;
        } else if (object instanceof LocalDate) {
            return (int) ((LocalDate) object).toEpochDay();
        } else {
            throw new IllegalArgumentException(
                    "Unexpected object type for DATE logical type. Received: " + object);
        }
    }

    private static int convertToTime(Object object) {
        final int millis;
        if (object instanceof Integer) {
            millis = (Integer) object;
        } else if (object instanceof LocalTime) {
            millis = ((LocalTime) object).get(ChronoField.MILLI_OF_DAY);
        } else {
            throw new IllegalArgumentException(
                    "Unexpected object type for TIME logical type. Received: " + object);
        }
        return millis;
    }

    private static byte[] convertToBytes(Object object) {
        if (object instanceof GenericFixed) {
            return ((GenericFixed) object).bytes();
        } else if (object instanceof ByteBuffer) {
            ByteBuffer byteBuffer = (ByteBuffer) object;
            byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes);
            return bytes;
        } else {
            return (byte[]) object;
        }
    }
}
