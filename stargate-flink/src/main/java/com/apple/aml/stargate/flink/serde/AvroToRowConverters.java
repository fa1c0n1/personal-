package com.apple.aml.stargate.flink.serde;

import com.apple.aml.stargate.pipeline.sdk.utils.JodaUtils;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.formats.avro.typeutils.AvroSchemaConverter.extractValueTypeToAvroMap;

/** Tool class used to convert from Avro {@link GenericRecord} to {@link RowData}. * */
@Internal
public class AvroToRowConverters {

    /**
     * Runtime converter that converts Avro data structures into objects of Flink Table & SQL
     * internal data structures.
     */
    @FunctionalInterface
    public interface AvroToRowConverter extends Serializable {
        Object convert(Object object);
    }

    // -------------------------------------------------------------------------------------
    // Runtime Converters
    // -------------------------------------------------------------------------------------

    public static AvroToRowConverter createRowConverter(RowType rowType) {
        final AvroToRowConverter[] fieldConverters =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .map(AvroToRowConverters::createNullableConverter)
                        .toArray(AvroToRowConverter[]::new);
        final int arity = rowType.getFieldCount();

        return avroObject -> {
            IndexedRecord record = (IndexedRecord) avroObject;
            Row row = new Row(arity);
            for (int i = 0; i < arity; ++i) {
                // avro always deserialize successfully even though the type isn't matched
                // so no need to throw exception about which field can't be deserialized
                row.setField(i, fieldConverters[i].convert(record.get(i)));
            }
            return row;
        };
    }

    /**
     * Creates a runtime converter which is null safe.
     */
    private static AvroToRowConverter createNullableConverter(LogicalType type) {
        final AvroToRowConverter converter = createConverter(type);
        return avroObject -> {
            if (avroObject == null) {
                return null;
            }
            return converter.convert(avroObject);
        };
    }

    /**
     * Creates a runtime converter which assuming input object is not null.
     */
    private static AvroToRowConverter createConverter(LogicalType type) {
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
                return AvroToRowConverters::convertToDate;
            case TIME_WITHOUT_TIME_ZONE:
                return AvroToRowConverters::convertToTime;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return AvroToRowConverters::convertToTimestamp;
            case CHAR:
            case VARCHAR:
                //TODO handle int to string.
                return avroObject -> avroObject.toString();
            case BINARY:
            case VARBINARY:
                return AvroToRowConverters::convertToBytes;
            case DECIMAL:
                return createDecimalConverter((DecimalType) type);
            case ARRAY:
                return createArrayConverter((ArrayType) type);
            case ROW:
                return createRowConverter((RowType) type);
            case MAP:
            case MULTISET:
                return createMapConverter(type);
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private static AvroToRowConverter createDecimalConverter(DecimalType decimalType) {
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

    private static AvroToRowConverter createArrayConverter(ArrayType arrayType) {
        final AvroToRowConverter elementConverter =
                createNullableConverter(arrayType.getElementType());
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

    private static AvroToRowConverter createMapConverter(LogicalType type) {
        final AvroToRowConverter keyConverter =
                createConverter(DataTypes.STRING().getLogicalType());
        final AvroToRowConverter valueConverter =
                createNullableConverter(extractValueTypeToAvroMap(type));

        return avroObject -> {
            final Map<?, ?> map = (Map<?, ?>) avroObject;
            Map<Object, Object> result = new HashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                Object key = keyConverter.convert(entry.getKey());
                Object value = valueConverter.convert(entry.getValue());
                result.put(key, value);
            }
            return result;
        };
    }

    private static TimestampData convertToTimestamp(Object object) {
        final long millis;
        if (object instanceof Long) {
            millis = (Long) object;
        } else if (object instanceof Instant) {
            millis = ((Instant) object).toEpochMilli();
        } else {
            JodaUtils jodaConverter = JodaUtils.getConverter();
            if (jodaConverter != null) {
                millis = jodaConverter.convertTimestamp(object);
            } else {
                throw new IllegalArgumentException(
                        "Unexpected object type for TIMESTAMP logical type. Received: " + object);
            }
        }
        return TimestampData.fromEpochMillis(millis);
    }

    private static int convertToDate(Object object) {
        if (object instanceof Integer) {
            return (Integer) object;
        } else if (object instanceof LocalDate) {
            return (int) ((LocalDate) object).toEpochDay();
        } else {
            JodaUtils jodaConverter = JodaUtils.getConverter();
            if (jodaConverter != null) {
                return (int) jodaConverter.convertDate(object);
            } else {
                throw new IllegalArgumentException(
                        "Unexpected object type for DATE logical type. Received: " + object);
            }
        }
    }

    private static int convertToTime(Object object) {
        final int millis;
        if (object instanceof Integer) {
            millis = (Integer) object;
        } else if (object instanceof LocalTime) {
            millis = ((LocalTime) object).get(ChronoField.MILLI_OF_DAY);
        } else {
            JodaUtils jodaConverter = JodaUtils.getConverter();
            if (jodaConverter != null) {
                millis = jodaConverter.convertTime(object);
            } else {
                throw new IllegalArgumentException(
                        "Unexpected object type for TIME logical type. Received: " + object);
            }
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
