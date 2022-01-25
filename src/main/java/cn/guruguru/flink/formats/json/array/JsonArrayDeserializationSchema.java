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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.lang.String.format;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * JSON Array 反序列化： json array string -> RowData
 */
public class JsonArrayDeserializationSchema implements DeserializationSchema<RowData> {
    private static final long serialVersionUID = 1L;

    public static final String JACKSON_OPTION_PREFIX = "jackson.";

    /** Flag indicating whether to fail if a field is missing. */
    private final boolean failOnMissingField;

    /** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
    private final boolean ignoreParseErrors;

    /** Flag indicating whether to remove duplicates for json array. */
    private final boolean removeDuplicates;

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
            boolean removeDuplicates,
            Map<String, Boolean> jacksonOptionMap) {
        if (ignoreParseErrors && failOnMissingField) {
            throw new IllegalArgumentException(
                    "JSON format doesn't support failOnMissingField and ignoreParseErrors are both enabled.");
        }
        this.resultTypeInfo = checkNotNull(resultTypeInfo);
        this.timestampFormat = timestampFormat;        // 'json-array.timestamp-format.standard'
        this.failOnMissingField = failOnMissingField;  // 'json-array.fail-on-missing-field'
        this.ignoreParseErrors = ignoreParseErrors;    // 'json-array.ignore-parse-errors'
        this.removeDuplicates = removeDuplicates;      // 'json-array.remove-duplicates'
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
            String jacksonKey = formatKeyOfOption(k, JACKSON_OPTION_PREFIX);
            Arrays.stream(DeserializationFeature.values())
                .filter(feature -> feature.toString().equals(jacksonKey))
                .forEach(feature -> objectMapper.configure(feature, v)); // configure(DeserializationFeature.valueOf(jacksonKey), v);
        });
    }

    /**
     * 将 JSON 数组字符串转化成 JSON 字符串数组，再逐一反序列化成 RowData
     *
     * 注：
     *
     * 1. Jackson 无法一步完成 JSON 数组字符串转化成 JSON 字符串数组
     *     即 String[] jsonStrArray = objectMapper.readValue(messages, String[].class)
     * 2. 没有使用 `Arrays.stream(objects).map(this::deserializeToRowData).filter(Objects::nonNull).distinct().forEach(out::collect);`
     *    进行去重，是因为 Lambda 中必须处理在中间的算子中处理完异常，而实际需要统一处理 JSON 数组及其元素的相关异常
     *
     *
     * @param messages serialized JSON array
     */
    @Override
    public void deserialize(byte[] messages, Collector<RowData> out) throws IOException {
        try {
            // 解组：将 JSON 数组字符串转化成 Object 数组，每个数组元素是一个 JSON 对象
            Object[] objects = objectMapper.readValue(messages, Object[].class); // throws some exceptions
            // 存放出现过的元素
            HashMap<Integer, RowData> map = new HashMap<>();

            for (Object object : objects) {
                // 编组：将 objects 元素逐一编组成 JSON 字符串（byte[] 类型）
                byte[] message = objectMapper.writeValueAsBytes(object); // throws JsonProcessingException
                // 反序列化：由 JsonRowDataDeserializationSchema 将 JSON 字符串（byte[] 类型）反序列化为 RowData
                RowData rowData = jsonDeserializer.deserialize(message); // throws IOException

                if (removeDuplicates) {
                    // 如果还没有出现过则进行收集
                    if (map.get(rowData.hashCode()) == null) {
                        out.collect(rowData);
                    }
                    map.put(rowData.hashCode(), rowData);
                } else {
                    // 逐一收集
                    out.collect(rowData);
                }
            }

            // 清空
            map.clear();
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
        return resultTypeInfo;
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

    // ------------------------------------------

    /**
     * @param key 'jackson.accept-single-value-as-array'
     * @return 'ACCEPT_SINGLE_VALUE_AS_ARRAY'
     */
    private String formatKeyOfOption(String key, String prefix) {
        if (key.startsWith(prefix)) {
            return key
                    .substring(prefix.length())
                    .replaceAll("-", "_")
                    .toUpperCase();
        }
        return key;
    }
}
