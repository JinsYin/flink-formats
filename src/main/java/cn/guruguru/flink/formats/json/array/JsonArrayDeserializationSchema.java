package cn.guruguru.flink.formats.json.array;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import static java.lang.String.format;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * JSON Array 反序列化： json array string -> RowData
 */
public class JsonArrayDeserializationSchema implements DeserializationSchema<RowData> {
    private static final long serialVersionUID = 1L;

    /** Flag indicating whether to fail if a field is missing. */
    private final boolean failOnMissingField;

    /** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
    private final boolean ignoreParseErrors;

    /** Timestamp format specification which is used to parse timestamp. */
    private final TimestampFormat timestampFormat;

    /** Flag accepting single value as array **/
    private final Map<String, Boolean> jacksonOptionMap;

    // ----------------------------------------

    /** The deserializer to deserialize JSON data. */
    private final JsonRowDataDeserializationSchema jsonDeserializer;

    /** TypeInformation of the produced {@link RowData}. **/
    private final TypeInformation<RowData> resultTypeInfo;

    ObjectMapper objectMapper = new ObjectMapper();

    public JsonArrayDeserializationSchema(
            RowType rowType,
            TypeInformation<RowData> resultTypeInfo,
            TimestampFormat timestampFormat,
            boolean failOnMissingField,
            boolean ignoreParseErrors,
            Map<String, Boolean> jacksonOptionMap) {
        if (ignoreParseErrors && failOnMissingField) {
            throw new IllegalArgumentException(
                    "JSON format doesn't support failOnMissingField and ignoreParseErrors are both enabled.");
        }
        this.resultTypeInfo = checkNotNull(resultTypeInfo);
        this.timestampFormat = timestampFormat;        // 'json-array.timestamp-format.standard'
        this.failOnMissingField = failOnMissingField;  // 'json-array.fail-on-missing-field'
        this.ignoreParseErrors = ignoreParseErrors;    // 'json-array.ignore-parse-errors'
        this.jacksonOptionMap = jacksonOptionMap;      // 'json-array.jackson.*'
        this.jsonDeserializer = new JsonRowDataDeserializationSchema(
            rowType,
            resultTypeInfo,
            failOnMissingField,
            ignoreParseErrors,
            timestampFormat
        );
    }

    /**
     * 开启 {@link ObjectMapper} 配置选项
     *
     * {@link DeserializationFeature}
     */
    @Override
    public void open(InitializationContext context) throws Exception {
        jacksonOptionMap.forEach((k, v) -> {
            String jacksonKey = convertKeyOfConfigOption(k);
            Arrays.stream(DeserializationFeature.values())
                .filter(feature -> feature.toString().equals(jacksonKey))
                .forEach(feature -> objectMapper.configure(feature, v)); // configure(DeserializationFeature.valueOf(jacksonKey), v);
        });
    }

    /**
     * 将 JSON 数组字符串转化成 JSON 字符串数组，再逐一反序列化成 RowData
     *
     * 注： Jackson 无法一步完成 JSON 数组字符串转化成 JSON 字符串数组
     *     即 String[] jsonStrArray = objectMapper.readValue(messages, String[].class)
     *
     * @param messages serialized JSON array
     */
    @Override
    public void deserialize(byte[] messages, Collector<RowData> out) throws IOException {
        try {
            // 解组：将 JSON 数组字符串转化成 Object 数组，每个数组元素是一个 JSON 对象
            Object[] objects = objectMapper.readValue(messages, Object[].class); // throws some exceptions

            for (Object object : objects) {
                // 编组：将 objects 元素逐一编组成 JSON 字符串（byte[] 类型）
                byte[] message = objectMapper.writeValueAsBytes(object); // throws JsonProcessingException

                // 反序列化：由 JsonRowDataDeserializationSchema 将 JSON 字符串（byte[] 类型）反序列化为 RowData
                RowData rowData = jsonDeserializer.deserialize(message); // throws IOException

                out.collect(rowData);
            }
        } catch (Throwable t) {
            if (!ignoreParseErrors) {
                throw new IOException(format("Failed to deserialize JSON array '%s'.", new String(messages)), t);
            }
        }
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        throw new RuntimeException(
                "Please invoke DeserializationSchema#deserialize(byte[], Collector<RowData>) instead.");
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return null;
    }

    // -------------------- Object --------------------

    /**
     * {@see JsonArrayFormatFactoryTest#testSeDeSchema assertEquals}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JsonArrayDeserializationSchema that = (JsonArrayDeserializationSchema) o;
        return failOnMissingField == that.failOnMissingField &&
                ignoreParseErrors == that.ignoreParseErrors &&
                resultTypeInfo.equals(that.resultTypeInfo) &&
                timestampFormat.equals(that.timestampFormat);
    }

    @Override
    public int hashCode() {
        return Objects.hash(failOnMissingField, ignoreParseErrors, resultTypeInfo, timestampFormat);
    }

    // ----------------------------------------

    /**
     * @param keyOfConfigOption 'jackson.accept-single-value-as-array'
     * @return 'ACCEPT_SINGLE_VALUE_AS_ARRAY'
     */
    private String convertKeyOfConfigOption(String keyOfConfigOption) {
        if (keyOfConfigOption.startsWith("jackson.")) {
            return keyOfConfigOption.substring(8).replaceAll("-", "_").toUpperCase();
        }
        return keyOfConfigOption;
    }
}
