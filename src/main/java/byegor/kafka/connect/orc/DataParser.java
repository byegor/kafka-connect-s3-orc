package byegor.kafka.connect.orc;

import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class DataParser {

    public void writeStructDataToOrc(ColumnVector[] orcColumns, Struct data, int rowIndex) {
        List<Field> schemaFields = data.schema().fields();
        for (int i = 0; i < orcColumns.length; i++) {
            ColumnVector column = orcColumns[i];
            Field field = schemaFields.get(i);
            writeFieldValue(column, field.schema(), data.get(field), rowIndex);
        }
    }

    private void writeFieldValue(ColumnVector column, Schema connectFieldSchema,
                                 Object fieldData, int rowIndex) {
        if (fieldData == null) {
            setNullData(column, rowIndex);
        } else {
            ColumnVector.Type type = column.type;
            switch (type) {
                case LONG:
                    ((LongColumnVector) column)
                            .vector[rowIndex] = (Long) parseData(connectFieldSchema, fieldData);
                    break;
                case BYTES:
                    ((BytesColumnVector) column)
                            .setVal(rowIndex, (byte[]) parseData(connectFieldSchema, fieldData));
                    break;
                case DOUBLE:
                    ((DoubleColumnVector) column)
                            .vector[rowIndex] = (Double) parseData(connectFieldSchema, fieldData);
                    break;
                case TIMESTAMP:
                    ((TimestampColumnVector) column).set(
                            rowIndex,
                            new java.sql.Timestamp((Long) parseData(connectFieldSchema, fieldData))
                    );
                    break;
                case LIST:
                    ListColumnVector listColumn = (ListColumnVector) column;
                    listColumn.offsets[rowIndex] = listColumn.childCount;
                    listColumn.lengths[rowIndex] = ((Collection) fieldData).size();
                    listColumn.childCount += ((Collection) fieldData).size();

                    ColumnVector childListColumn = listColumn.child;
                    Schema childFieldSchema = connectFieldSchema.valueSchema();
                    int currentPosition = (int) listColumn.offsets[rowIndex];
                    for (Object object : ((Collection) fieldData)) {
                        writeFieldValue(childListColumn, childFieldSchema, object, currentPosition++);
                    }
                    break;
                case MAP:
                    MapColumnVector mapColumn = (MapColumnVector) column;

                    mapColumn.offsets[rowIndex] = mapColumn.childCount;
                    mapColumn.lengths[rowIndex] = ((Map) fieldData).size();
                    mapColumn.childCount += ((Map) fieldData).size();

                    ColumnVector keyColumn = mapColumn.keys;
                    ColumnVector valueColumn = mapColumn.values;

                    int startPosition = (int) mapColumn.offsets[rowIndex];
                    for (Object entry : ((Map) fieldData).entrySet()) {
                        int pos = startPosition++;
                        Object key = ((Map.Entry) entry).getKey();
                        writeFieldValue(keyColumn, connectFieldSchema.keySchema(), key, pos);
                        Object value = ((Map.Entry) entry).getValue();
                        writeFieldValue(valueColumn, connectFieldSchema.valueSchema(), value, pos);
                    }
                    break;
                case STRUCT:
                    StructColumnVector structColumn = (StructColumnVector) column;
                    writeStructDataToOrc(structColumn.fields, (Struct) fieldData, rowIndex);
                    break;
                default:
                    throw new DataException("Unsupported orc schema type:" + type);
            }
        }
    }

    /**
     * Mark that current column on row has null as value, mark whole column as nullable
     */
    private void setNullData(ColumnVector column, int rowIndex) {
        column.isNull[rowIndex] = true;
        column.noNulls = false;
    }


    private Object parseData(Schema connectSchema, Object fieldValue) {
        Schema.Type type = connectSchema.type();
        switch (type) {
            case FLOAT64:
            case STRUCT:
                return fieldValue;
            case BYTES:
                return ((ByteBuffer) fieldValue).array();
            case BOOLEAN:
                return ((Boolean) fieldValue) ? 1L : 0L;
            case FLOAT32:
                return Double.valueOf(fieldValue.toString());
            case INT8:
                return ((Byte) fieldValue).longValue();
            case INT16:
                return ((Short) fieldValue).longValue();
            case INT32:
                return ((Integer) fieldValue).longValue();
            case INT64:
                String name = connectSchema.name();
                if (org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME.equals(name)) {
                    return ((Date) fieldValue).getTime();
                } else {
                    return fieldValue;
                }
            case STRING:
                return ((String) fieldValue).getBytes(StandardCharsets.UTF_8);
            case ARRAY:
                Schema listType = connectSchema.valueSchema();
                List<Object> transformedValues = new ArrayList<>(((List) fieldValue).size());
                for (Object value : ((List) fieldValue)) {
                    transformedValues.add(parseData(listType, value));
                }
                return transformedValues;
            case MAP:
                Schema keySchema = connectSchema.keySchema();
                Schema valueSchema = connectSchema.valueSchema();
                Map<Object, Object> transformedMap = new HashMap<>();
                for (Object entry : ((Map) fieldValue).entrySet()) {
                    Object key = parseData(keySchema, ((Map.Entry) entry).getKey());
                    Object value = parseData(valueSchema, ((Map.Entry) entry).getValue());
                    transformedMap.put(key, value);
                }
                return transformedMap;
            default:
                throw new DataException("Unsupported connect schema type:" + connectSchema);
        }
    }
}
