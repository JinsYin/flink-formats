package cn.guruguru.flink.formats.json.array;

import org.apache.flink.formats.json.JsonRowDataSerializationSchema;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static cn.guruguru.flink.formats.json.array.JsonArrayFormatFactory.JACKSON_ACCEPT_SINGLE_VALUE_AS_ARRAY;
import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.Assert.assertEquals;

public class JsonArraySerDeSchemaTest {

    /**
     * 测试 JSON 数组字符串转 JSON 字符串数组
     */
    @Test
    public void testConvertJsonArrStrToJsonStrArr() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        String jsonArrStr = "[{\"bool\":true,\"tinyint\":99,\"name\":\"alice\"},{\"bool\":false,\"tinyint\":100,\"name\":\"bob\"}]";
        String[] jsonStrArr = {"{\"bool\":true,\"tinyint\":99,\"name\":\"alice\"}", "{\"bool\":false,\"tinyint\":100,\"name\":\"bob\"}"};

        Object[] objects = objectMapper.readValue(jsonArrStr, Object[].class);

        for (int i = 0; i < objects.length; i++) {
            String jsonStr = objectMapper.writeValueAsString(objects[i]);
            assertEquals(jsonStrArr[i], jsonStr);
        }
    }

    @Test
    public void testConvertJsonArrStrToJsonStrArr2() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);

        String json = "{\"bool\":true,\"tinyint\":99,\"name\":\"alice\"}";
        Object[] objects = objectMapper.readValue(json, Object[].class);

        assertEquals(objects.length, 1);

        for (Object object : objects) {
            String serializedJson = objectMapper.writeValueAsString(object);
            assertEquals(serializedJson, json);
        }
    }

    @Test
    public void testDeserialize() throws Exception {
        byte tinyint = 'c';
        short smallint = 128;
        int intValue = 45536;
        float floatValue = 33.333F;
        long bigint = 1238123899121L;
        String name = "asdlkjasjkdla998y1122";
        byte[] bytes = new byte[1024];
        ThreadLocalRandom.current().nextBytes(bytes);
        BigDecimal decimal = new BigDecimal("123.456789");
        Double[] doubles = new Double[]{1.1, 2.2, 3.3};
        LocalDate date = LocalDate.parse("1990-10-14");
        LocalTime time = LocalTime.parse("12:12:43");
        Timestamp timestamp3 = Timestamp.valueOf("1990-10-14 12:12:43.123");
        Timestamp timestamp9 = Timestamp.valueOf("1990-10-14 12:12:43.123456789");

        Map<String, Long> map = new HashMap<>();
        map.put("flink", 123L);

        Map<String, Map<String, Integer>> nestedMap = new HashMap<>();
        Map<String, Integer> innerMap = new HashMap<>();
        innerMap.put("key", 234);
        nestedMap.put("inner_map", innerMap);

        ObjectMapper objectMapper = new ObjectMapper();

        ObjectNode objectNode1 = objectMapper.createObjectNode();
        objectNode1.put("bool", true);
        objectNode1.put("tinyint", tinyint);
        objectNode1.put("smallint", smallint);
        objectNode1.put("int", intValue);
        objectNode1.put("bigint", bigint);
        objectNode1.put("float", floatValue);
        objectNode1.put("name", name);
        objectNode1.put("bytes", bytes);
        objectNode1.put("decimal", decimal);
        objectNode1.set("doubles", objectMapper.createArrayNode().add(1.1D).add(2.2D).add(3.3D));
        objectNode1.put("date", "1990-10-14");
        objectNode1.put("time", "12:12:43");
        objectNode1.put("timestamp3", "1990-10-14T12:12:43.123");
        objectNode1.put("timestamp9", "1990-10-14T12:12:43.123456789");
        objectNode1.putObject("map").put("flink", 123);
        objectNode1.putObject("map2map").putObject("inner_map").put("key", 234);

        ObjectNode objectNode2 = objectMapper.createObjectNode();
        objectNode2.put("bool", false);
        objectNode2.put("tinyint", tinyint);
        objectNode2.put("smallint", smallint);
        objectNode2.put("int", intValue);
        objectNode2.put("bigint", bigint);
        objectNode2.put("float", floatValue);
        objectNode2.put("name", name);
        objectNode2.put("bytes", bytes);
        objectNode2.put("decimal", decimal);
        objectNode2.set("doubles", objectMapper.createArrayNode().add(1.1D).add(2.2D).add(3.3D));
        objectNode2.put("date", "1990-10-14");
        objectNode2.put("time", "12:12:43");
        objectNode2.put("timestamp3", "1990-10-14T12:12:43.123");
        objectNode2.put("timestamp9", "1990-10-14T12:12:43.123456789");
        objectNode2.putObject("map").put("flink", 123);
        objectNode2.putObject("map2map").putObject("inner_map").put("key", 234);

        // 模拟 JSON 数组
        ArrayNode arrayNode = objectMapper.createArrayNode().add(objectNode1).add(objectNode2);

        byte[] serializedJsonArray = objectMapper.writeValueAsBytes(arrayNode);

        // Data type of the element of json array
        DataType dataType = ROW(
                FIELD("bool", BOOLEAN()),
                FIELD("tinyint", TINYINT()),
                FIELD("smallint", SMALLINT()),
                FIELD("int", INT()),
                FIELD("bigint", BIGINT()),
                FIELD("float", FLOAT()),
                FIELD("name", STRING()),
                FIELD("bytes", BYTES()),
                FIELD("decimal", DECIMAL(9, 6)),
                FIELD("doubles", ARRAY(DOUBLE())),
                FIELD("date", DATE()),
                FIELD("time", TIME(0)),
                FIELD("timestamp3", TIMESTAMP(3)),
                FIELD("timestamp9", TIMESTAMP(9)),
                FIELD("map", MAP(STRING(), BIGINT())),
                FIELD("map2map", MAP(STRING(), MAP(STRING(), INT())))
        );

        RowType rowType = (RowType) dataType.getLogicalType();
        RowDataTypeInfo resultTypeInfo = new RowDataTypeInfo(rowType);

        final Map<String, Boolean> jacksonOptionMap = new HashMap<>();
        jacksonOptionMap.put(JACKSON_ACCEPT_SINGLE_VALUE_AS_ARRAY.key(), false);

        JsonArrayDeserializationSchema deserializationSchema = new JsonArrayDeserializationSchema(
                rowType, resultTypeInfo,  TimestampFormat.ISO_8601,false, false,  jacksonOptionMap);

        // 对应于 SQL 的 ROW 类型，外部数据类型
        Row expectedRow1 = new Row(16);
        expectedRow1.setField(0, true);
        expectedRow1.setField(1, tinyint);
        expectedRow1.setField(2, smallint);
        expectedRow1.setField(3, intValue);
        expectedRow1.setField(4, bigint);
        expectedRow1.setField(5, floatValue);
        expectedRow1.setField(6, name);
        expectedRow1.setField(7, bytes);
        expectedRow1.setField(8, decimal);
        expectedRow1.setField(9, doubles);
        expectedRow1.setField(10, date);
        expectedRow1.setField(11, time);
        expectedRow1.setField(12, timestamp3.toLocalDateTime());
        expectedRow1.setField(13, timestamp9.toLocalDateTime());
        expectedRow1.setField(14, map);
        expectedRow1.setField(15, nestedMap);

        Row expectedRow2 = new Row(16);
        expectedRow2.setField(0, false);
        expectedRow2.setField(1, tinyint);
        expectedRow2.setField(2, smallint);
        expectedRow2.setField(3, intValue);
        expectedRow2.setField(4, bigint);
        expectedRow2.setField(5, floatValue);
        expectedRow2.setField(6, name);
        expectedRow2.setField(7, bytes);
        expectedRow2.setField(8, decimal);
        expectedRow2.setField(9, doubles);
        expectedRow2.setField(10, date);
        expectedRow2.setField(11, time);
        expectedRow2.setField(12, timestamp3.toLocalDateTime());
        expectedRow2.setField(13, timestamp9.toLocalDateTime());
        expectedRow2.setField(14, map);
        expectedRow2.setField(15, nestedMap);

        Row[] expectedRowArray = new Row[]{expectedRow1, expectedRow2};

        deserializationSchema.deserialize(serializedJsonArray, new Collector<RowData>() {
            int idx = 0;

            @Override
            public void collect(RowData rowData) {
                // test deserialization
                Row actual = convertToExternal(rowData, dataType);
                assertEquals(expectedRowArray[idx], actual);

                // test serialization
                JsonRowDataSerializationSchema serializationSchema = new JsonRowDataSerializationSchema(rowType,  TimestampFormat.ISO_8601);
                byte[] actualBytes = serializationSchema.serialize(rowData);
                assertEquals(String.valueOf(arrayNode.get(idx)), new String(actualBytes));

                idx += 1;
            }

            @Override
            public void close() {}
        });
    }

    @SuppressWarnings("unchecked")
    private static Row convertToExternal(RowData rowData, DataType dataType) {
        return (Row) DataFormatConverters.getConverterForDataType(dataType).toExternal(rowData);
    }

}
