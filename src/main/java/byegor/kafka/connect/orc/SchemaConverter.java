package byegor.kafka.connect.orc;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.orc.TypeDescription;

import java.util.List;

public class SchemaConverter {
    public static TypeDescription toOrcSchema(Schema connectSchema) {
        List<Field> fields = connectSchema.fields();
        return toOrcStruct(fields);
    }

    private static TypeDescription toOrcStruct(List<Field> fields) {
        TypeDescription struct = TypeDescription.createStruct();
        for (Field field : fields) {
            struct.addField(field.name(), toOrcField(field.schema()));
        }
        return struct;

    }

    private static TypeDescription toOrcField(Schema fieldSchema) {
        Schema.Type fieldType = fieldSchema.type();
        switch (fieldType) {
            case BOOLEAN:
                return TypeDescription.createBoolean();
            case BYTES:
                return TypeDescription.createBinary();
            case INT8:
                return TypeDescription.createByte();
            case INT16:
                return TypeDescription.createShort();
            case INT32:
                return TypeDescription.createInt();
            case INT64:
                if (Timestamp.LOGICAL_NAME.equals(fieldSchema.name())) {
                    return TypeDescription.createTimestamp();
                } else {
                    return TypeDescription.createLong();
                }
            case STRING:
                return TypeDescription.createString();
            case FLOAT64:
                return TypeDescription.createDouble();
            case FLOAT32:
                return TypeDescription.createFloat();
            case MAP:
                return TypeDescription.createMap(
                        toOrcField(fieldSchema.keySchema()),
                        toOrcField(fieldSchema.valueSchema())
                );
            case ARRAY:
                return TypeDescription.createList(toOrcField(fieldSchema.valueSchema()));
            case STRUCT:
                List<Field> fields = fieldSchema.fields();
                return toOrcStruct(fields);
            default:
                throw new DataException("Unsupported type: " + fieldType);
        }
    }
}
