package cn.guruguru.flink.formats.json.ogg;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Objects;

import static java.lang.String.format;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

public class OggJsonDeserializationSchema  implements DeserializationSchema<RowData>  {
    private static final long serialVersionUID = 1L;

    private static final String OP_INSERT = "I";
    private static final String OP_UPDATE = "U";
    private static final String OP_DELETE = "D";

    /** The deserializer to deserialize OGG JSON data. */
    private final JsonRowDataDeserializationSchema jsonDeserializer;

    /** TypeInformation of the produced {@link RowData}. **/
    private final TypeInformation<RowData> resultTypeInfo;

    /** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
    private final boolean ignoreParseErrors;

    /** Number of fields. 即 {@link #createJsonRowType(DataType)} 指定的字段数量 */
    private final int fieldCount;

    /**
     * {@link JsonRowDataDeserializationSchema#JsonRowDataDeserializationSchema(RowType, TypeInformation, boolean, boolean, TimestampFormat)}
     */
    public OggJsonDeserializationSchema(
            RowType rowType,
            TypeInformation<RowData> resultTypeInfo,
            boolean ignoreParseErrors,
            TimestampFormat timestampFormatOption) {
        this.resultTypeInfo = resultTypeInfo;
        this.ignoreParseErrors = ignoreParseErrors;
        this.fieldCount = rowType.getFieldCount();
        this.jsonDeserializer = new JsonRowDataDeserializationSchema(
                createJsonRowType(fromLogicalToDataType(rowType)),
                // the result type is never used, so it's fine to pass in Canal's result type
                resultTypeInfo,
                false, // ignoreParseErrors already contains the functionality of failOnMissingField
                ignoreParseErrors,
                timestampFormatOption);
    }

    // --------------- DeserializationSchema ---------------

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        throw new RuntimeException(
            "Please invoke DeserializationSchema#deserialize(byte[], Collector<RowData>) instead.");
    }

    /**
     * row = (op_type, before, after)
     * {@link #createJsonRowType(DataType)}
     */
    @Override
    public void deserialize(byte[] message, Collector<RowData> out) throws IOException {
        try {
            RowData row = jsonDeserializer.deserialize(message);
            String opType = row.getString(0).toString(); // "op_type" field
            if (OP_INSERT.equals(opType)) {
                RowData insert = row.getRow(2, fieldCount); // "after" field. At the same time, the "before" field is null
                insert.setRowKind(RowKind.INSERT);
                out.collect(insert);
            } else if (OP_UPDATE.equals(opType)) {
                // the underlying JSON deserialization schema always produce GenericRowData.
                GenericRowData before = (GenericRowData) row.getRow(1, fieldCount); // "before" field, it is a empty json
                GenericRowData after = (GenericRowData) row.getRow(2, fieldCount);  // "after" field
                for (int f = 0; f < fieldCount; f++) {
                    // "before":{}
                    if (before.isNullAt(f)) {
                        // not empty fields in "before" means the fields are changed
                        // empty fields in "before" means the fields are not changed
                        // so we just copy the not changed fields into before
                        before.setField(f, after.getField(f));
                    }
                }
                before.setRowKind(RowKind.UPDATE_BEFORE);
                after.setRowKind(RowKind.UPDATE_AFTER);
                out.collect(before);
                out.collect(after);
            } else if (OP_DELETE.equals(opType)) {
                RowData delete = row.getRow(1, fieldCount); // "before" field. At the same time, the "after" field is null
                delete.setRowKind(RowKind.DELETE);
                out.collect(delete);
            } else {
                if (!ignoreParseErrors) {
                    throw new IOException(format(
                            "Unknown \"op_type\" value \"%s\". The OGG JSON message is '%s'", opType, new String(message)));
                }
            }

        } catch (Throwable t) {
            // a big try catch to protect the processing.
            if (!ignoreParseErrors) {
                throw new IOException(format(
                        "Corrupt OGG JSON message '%s'.", new String(message)), t);
            }
        }
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    // --------------- ResultTypeQueryable ---------------

    @Override
    public TypeInformation<RowData> getProducedType() {
        return resultTypeInfo;
    }

    // -------------------- Object --------------------

    /**
     * 判断两个对象是否相等，包括引用相等和值相等
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OggJsonDeserializationSchema that = (OggJsonDeserializationSchema) o;
        return ignoreParseErrors == that.ignoreParseErrors &&
                fieldCount == that.fieldCount &&
                Objects.equals(jsonDeserializer, that.jsonDeserializer) &&
                Objects.equals(resultTypeInfo, that.resultTypeInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jsonDeserializer, resultTypeInfo, ignoreParseErrors, fieldCount);
    }

    // ----------------------------------------

    private RowType createJsonRowType(DataType databaseSchema) {
        // OGG JSON contains other information, e.g. "table", "op_ts", "current_ts", "pos"
        // but we don't need them
        return (RowType) DataTypes.ROW(
            // The following annotated fields are not required.
            // DataTypes.FIELD("table", DataTypes.STRING()),
            // DataTypes.FIELD("op_ts", DataTypes.TIME(6)),
            // DataTypes.FIELD("current_ts", DataTypes.TIME(6)),
            // DataTypes.FIELD("pos", DataTypes.STRING()),
            DataTypes.FIELD("op_type", DataTypes.STRING()),
            DataTypes.FIELD("before", databaseSchema),
            DataTypes.FIELD("after", databaseSchema)
        ).getLogicalType();
    }
}
