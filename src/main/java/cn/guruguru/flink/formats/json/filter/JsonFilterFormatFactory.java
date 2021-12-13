package cn.guruguru.flink.formats.json.filter;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class JsonFilterFormatFactory {
    public static final String IDENTIFIER = "json-filter";

    // 'key=value' 仅支持等值过滤，值类型仅支持字符串和数字
    public static final ConfigOption<String> FILTER_CONDITION = ConfigOptions
            .key("condition")
            .stringType()
            .defaultValue("")
            .withDescription("Filter condition. Only equivalent filtering is supported.");
}
