package byegor.kafka.connect.orc;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.orc.TypeDescription;
import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.*;

public class SchemaConverterTest {

    @Test
    public void toOrcSchemaTest() {
        Schema connectSchema = createConnectSchema();
        TypeDescription orcSchema = SchemaConverter.toOrcSchema(connectSchema);

        TypeDescription expectedSchema = TypeDescription.createStruct()
                .addField("id", TypeDescription.createString())
                .addField("count", TypeDescription.createInt())
                .addField("longCount", TypeDescription.createLong())
                .addField("bool", TypeDescription.createBoolean())
                .addField("date", TypeDescription.createTimestamp())
                .addField("list", TypeDescription.createList(TypeDescription.createString()))
                .addField("map", TypeDescription.createMap(TypeDescription.createString(), TypeDescription.createLong()));
        assertEquals(expectedSchema, orcSchema);
    }


    public static Schema createConnectSchema() {
        SchemaBuilder builder = new SchemaBuilder(Schema.Type.STRUCT);
        builder.field("id", Schema.STRING_SCHEMA);
        builder.field("count", Schema.INT32_SCHEMA);
        builder.field("longCount", Schema.INT64_SCHEMA);
        builder.field("bool", Schema.BOOLEAN_SCHEMA);
        builder.field("date", Timestamp.SCHEMA);
        builder.field("list", SchemaBuilder.array(Schema.STRING_SCHEMA).build());
        builder.field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT64_SCHEMA).build());
        return builder.build();
    }

}