# `ogg-json` format

对于 OGG JSON，当 `"op_type":"U"` 时，`"before":{}`，即目前 OGG 捕获变更时并没有获取变更前的内容。

## 参考

* [Flink 源码 | 自定义 Format 消费 Maxwell CDC 数据](https://developer.aliyun.com/article/771438)
* [Support maxwell-json format to read Maxwell changelogs](https://github.com/apache/flink/pull/13090)
