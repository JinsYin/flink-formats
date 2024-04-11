# `json-array` format

```sql
CREATE TABLE JsonArrayTable(
  `bool` BOOLEAN,
  `tinyint` INT
) WITH (
  'connector' = 'kafka',
  'topic' = 'flink-sql',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'flink',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json-array',
  'json-array.timestamp-format.standard' = 'SQL',
  'json-array.ignore-parse-errors' = 'true',
  'json-array.fail-on-missing-field' = 'false',
  'json-array.remove-duplicates' = 'true'
);
```

```sql
SELECT * FROM JsonArrayTable;
```

---

```bash
[{"bool":true,"tinyint":99},{"bool":false,"tinyint":100}] # 录入

{"bool":false,"tinyint":80} # 不录入，不报错

123  # 不录入，不报错

[{"bool":true,"tinyint":89},{"bool":false,"tinyint":11}] # 录入
```

-----------

```sql
CREATE TABLE JsonArrayTable2 (
  `bool` BOOLEAN,
  `tinyint` INT
) WITH (
  'connector' = 'kafka',
  'topic' = 'flink-sql',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'flink',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json-array',
  'json-array.timestamp-format.standard' = 'SQL',
  'json-array.ignore-parse-errors' = 'false',
  'json-array.fail-on-missing-field' = 'false'
);

SELECT * FROM JsonArrayTable2;
```

```bash
[{"bool":true,"tinyint":99},{"bool":false,"tinyint":100}] # 录入

{"bool":false,"tinyint":80} # 报错

123  # 报错
```

---

```sql
CREATE TABLE JsonTable(
  `bool` BOOLEAN,
  `tinyint` INT
) WITH (
  'connector' = 'kafka',
  'topic' = 'flink-sql',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'flink',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json-array',
  'json-array.timestamp-format.standard' = 'SQL',
  'json-array.ignore-parse-errors' = 'true',
  'json-array.fail-on-missing-field' = 'false',
  'json-array.jackson.accept-single-value-as-array' = 'true'
);

SELECT * FROM JsonTable;
```

```bash
[{"bool":true,"tinyint":99},{"bool":false,"tinyint":100}] # 录入

{"bool":true,"tinyint":10} # 录入

123 # 不录入，不报错
```
