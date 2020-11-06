package cn.guruguru.flink.formats.json.ogg;

import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link OggJsonDeserializationSchema}.
 */
public class OggJsonDeserializationSchemaTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static final RowType SCHEMA = (RowType) ROW(
            FIELD("ID", INT().notNull()),
            FIELD("NAME", STRING()),
            FIELD("DESCRIPTION", STRING()),
            FIELD("WEIGHT", FLOAT())
    ).getLogicalType();

    @Test
    public void testDeserialization() throws Exception {
        List<String> lines = readLines("ogg-data.txt");
        OggJsonDeserializationSchema deserializationSchema = new OggJsonDeserializationSchema(
                SCHEMA,
                new RowDataTypeInfo(SCHEMA),
                false,
                TimestampFormat.ISO_8601);

        OggJsonDeserializationSchemaTest.SimpleCollector collector = new OggJsonDeserializationSchemaTest.SimpleCollector();
        for (String line : lines) {
            deserializationSchema.deserialize(line.getBytes(StandardCharsets.UTF_8), collector);
        }

        List<String> expected = Arrays.asList(
                "+I(102,car battery,12V car battery,5.17)",
                "+I(103,scooter,Small 2-wheel scooter,5.17)",
                "-U(102,car battery,12V car battery,5.15)", // 如果 update 类型的数据的 "before" 字段为空，这里填 5.15，否则填 5.17
                "+U(102,car battery,12V car battery,5.15)",
                "-D(102,car battery,12V car battery,5.15)"
        );
        assertEquals(expected, collector.list);
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    private static List<String> readLines(String resource) throws IOException {
        final URL url = OggJsonDeserializationSchemaTest.class.getClassLoader().getResource(resource);
        assert url != null;
        Path path = new File(url.getFile()).toPath();
        return Files.readAllLines(path);
    }

    private static class SimpleCollector implements Collector<RowData> {

        private List<String> list = new ArrayList<>();

        @Override
        public void collect(RowData record) {
            list.add(record.toString());
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
