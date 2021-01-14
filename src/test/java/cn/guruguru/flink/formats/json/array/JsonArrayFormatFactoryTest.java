package cn.guruguru.flink.formats.json.array;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TestDynamicTableFactory;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.TestLogger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static cn.guruguru.flink.formats.json.array.JsonArrayFormatFactory.JACKSON_ACCEPT_SINGLE_VALUE_AS_ARRAY;
import static org.apache.flink.util.CoreMatchers.containsCause;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link JsonArrayFormatFactory}
 */
public class JsonArrayFormatFactoryTest extends TestLogger {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static final TableSchema SCHEMA = TableSchema.builder()
            .field("a", DataTypes.STRING())
            .field("b", DataTypes.INT())
            .field("c", DataTypes.BOOLEAN())
            .build();

    private static final RowType ROW_TYPE = (RowType) SCHEMA.toRowDataType().getLogicalType();

    @Test
    public void testSeDeSchema() {
        final Map<String, Boolean> jacksonOptionMap = new HashMap<>();
        jacksonOptionMap.put(JACKSON_ACCEPT_SINGLE_VALUE_AS_ARRAY.key(), false);

        final JsonArrayDeserializationSchema expectedDeser = new JsonArrayDeserializationSchema(
                ROW_TYPE,
                new RowDataTypeInfo(ROW_TYPE),
                TimestampFormat.ISO_8601,
                false,
                true,
                jacksonOptionMap);

        final Map<String, String> options = getAllOptions();

        final DynamicTableSource actualSource = createTableSource(options);
        assert actualSource instanceof TestDynamicTableFactory.DynamicTableSourceMock;
        TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
                (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

        DeserializationSchema<RowData> actualDeser = scanSourceMock.valueFormat
                .createRuntimeDecoder(
                        ScanRuntimeProviderContext.INSTANCE,
                        SCHEMA.toRowDataType());

        assertEquals(expectedDeser, actualDeser);

        thrown.expect(containsCause(new UnsupportedOperationException(
                "Json array format doesn't support as a sink format yet.")));
        createTableSink(options);
    }

    @Test
    public void testInvalidIgnoreParseError() {
        thrown.expect(containsCause(new IllegalArgumentException(
                "Unrecognized option for boolean: abc. Expected either true or false(case insensitive)")));

        final Map<String, String> options =
                getModifiedOptions(opts -> opts.put("json-array.ignore-parse-errors", "abc"));

        createTableSource(options);
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    /**
     * Returns the full options modified by the given consumer {@code optionModifier}.
     *
     * @param optionModifier Consumer to modify the options
     */
    private Map<String, String> getModifiedOptions(Consumer<Map<String, String>> optionModifier) {
        Map<String, String> options = getAllOptions();
        optionModifier.accept(options);
        return options;
    }

    private Map<String, String> getAllOptions() {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", TestDynamicTableFactory.IDENTIFIER);
        options.put("target", "MyTarget");
        options.put("buffer-size", "1000");

        options.put("format", "json-array");
        options.put("json-array.fail-on-missing-field", "false");
        options.put("json-array.ignore-parse-errors", "true");
        options.put("json-array.timestamp-format.standard", "ISO-8601");
        return options;
    }

    private static DynamicTableSource createTableSource(Map<String, String> options) {
        return FactoryUtil.createTableSource(
                null,
                ObjectIdentifier.of("default", "default", "t1"),
                new CatalogTableImpl(SCHEMA, options, "mock source"),
                new Configuration(),
                JsonArrayFormatFactory.class.getClassLoader());
    }

    private static DynamicTableSink createTableSink(Map<String, String> options) {
        return FactoryUtil.createTableSink(
                null,
                ObjectIdentifier.of("default", "default", "t1"),
                new CatalogTableImpl(SCHEMA, options, "mock sink"),
                new Configuration(),
                JsonArrayFormatFactory.class.getClassLoader());
    }
}
