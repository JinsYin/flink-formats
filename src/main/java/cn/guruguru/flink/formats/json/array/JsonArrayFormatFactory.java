package cn.guruguru.flink.formats.json.array;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * JSON ARRAY 序列化和反序列化格式工厂
 */
public class JsonArrayFormatFactory implements
        DeserializationFormatFactory,
        SerializationFormatFactory {

    public static final String IDENTIFIER = "json-array";

    // --------------- JSON ARRAY Options -----------------

    public static final ConfigOption<Boolean> FAIL_ON_MISSING_FIELD = JsonOptions.FAIL_ON_MISSING_FIELD;

    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS = JsonOptions.IGNORE_PARSE_ERRORS;

    public static final ConfigOption<String> TIMESTAMP_FORMAT = JsonOptions.TIMESTAMP_FORMAT;

    /**
     * {@link org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature}
     */
    public static final ConfigOption<Boolean> JACKSON_ACCEPT_SINGLE_VALUE_AS_ARRAY = ConfigOptions
            .key("jackson.accept-single-value-as-array")
            .booleanType()
            .defaultValue(false)
            .withDescription("Optional flag to accept single value as array instead of failing; false by default");

    // --------- DeserializationFormatFactory / DecodingFormatFactory ---------

    @SuppressWarnings("unchecked")
    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context,
            ReadableConfig formatOptions) {
        // 验证工厂选项，即验证选项的可选和必选
        FactoryUtil.validateFactoryOptions(this, formatOptions);

        // 格式选项
        TimestampFormat timestampOption = JsonOptions.getTimestampFormat(formatOptions);
        final boolean failOnMissingField = formatOptions.get(FAIL_ON_MISSING_FIELD);
        final boolean ignoreParseErrors = formatOptions.get(IGNORE_PARSE_ERRORS);
        final Map<String, Boolean> jacksonOptionMap = getJacksonOptionMap(formatOptions);

        return new DecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(
                    DynamicTableSource.Context context,
                    DataType producedDataType) {
                final RowType rowType = (RowType) producedDataType.getLogicalType();
                final TypeInformation<RowData> rowDataTypeInfo =
                        (TypeInformation<RowData>) context.createTypeInformation(producedDataType);
                return new JsonArrayDeserializationSchema(
                    rowType,
                    rowDataTypeInfo,
                    timestampOption,    // 'json-array.timestamp-format.standard'
                    failOnMissingField, // 'json-array.fail-on-missing-field'
                    ignoreParseErrors,  // 'json-array.ignore-parse-errors'
                    jacksonOptionMap    // 'json-array.jackson.*'
                );
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    // ---------- SerializationFormatFactory / EncodingFormatFactory ----------

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
            DynamicTableFactory.Context context,
            ReadableConfig formatOptions) {
        throw new UnsupportedOperationException("Json array format doesn't support as a sink format yet.");
    }

    // -------------------- Factory --------------------

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        List<ConfigOption<?>> jacksonOptionList = getJacksonOptionList();
        options.add(TIMESTAMP_FORMAT);
        options.add(FAIL_ON_MISSING_FIELD);
        options.add(IGNORE_PARSE_ERRORS);
        options.addAll(jacksonOptionList);
        return options;
    }

    // --------------------------------------------------

    private Map<String, Boolean> getJacksonOptionMap(ReadableConfig formatOptions) {
        Map<String, Boolean> optionMap = new ConcurrentHashMap<>();
        List<ConfigOption<?>> jacksonOptionList = getJacksonOptionList();
        jacksonOptionList.forEach(option -> optionMap.put(option.key(), (Boolean) formatOptions.get(option)));
        return optionMap;
    }

    private List<ConfigOption<?>> getJacksonOptionList() {
        return Arrays.asList(
            JACKSON_ACCEPT_SINGLE_VALUE_AS_ARRAY
        );
    }
}
