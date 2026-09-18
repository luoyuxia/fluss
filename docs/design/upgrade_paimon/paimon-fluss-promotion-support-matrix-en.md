<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Paimon → Fluss Table Definition Mapping Matrix

This document evaluates whether existing Fluss capabilities can express Paimon table definitions, using concrete options to describe each mapping. A supported mapping does not imply that the current automatic conversion code implements it. All applicable conditions must hold for a table.

The first FIP-25 implementation phase supports only append-only Paimon tables promoted as Fluss log tables. The primary-key table analysis is retained for future work and does not describe the current implementation scope.

Code baseline (2026-09-08): Fluss `83181a74b` and local Paimon checkout `a3ce9f18a`. Fluss depends on Paimon 2.0.0, while the local Paimon project version is 1.5-SNAPSHOT. The following is a static comparison of their source code.

## 1. Log Tables

| Paimon definition / option | Can be mapped? | Corresponding Fluss definition / option and limitations |
| --- | --- | --- |
| No primary key declaration | Yes | A Fluss log table without a primary key preserves append semantics. |
| Unpartitioned table, or partitioning by one or more columns | Yes | Preserve partition columns and their order. Types and partition value encodings must be compatible. |
| `bucket=N`, `N>0`, `bucket-function.type=default`, `bucket-key=k1,k2` | Yes | Use the same `bucket.num=N` and bucket keys, with the corresponding Paimon hash function. The bucket count must satisfy the Fluss cluster limit. |
| `bucket=-1`, i.e. BUCKET_UNAWARE | Yes | Fluss uses a fixed positive `bucket.num`, while the Paimon lake table retains unaware mode. The Fluss bucket count does not have to be 1. |
| Fixed buckets with `bucket-function.type=mod` | No direct mapping | Fluss currently has no implementation corresponding to Paimon's mod hash. |
| Nonempty `upsert-key` | Cannot map to a log table with equivalent semantics | Fluss log tables have no definition for updating existing rows by this key. |
| Nonempty `rowkind.field` | Cannot map through the table definition alone | No corresponding Fluss table property interprets a data column as INSERT/DELETE operations. |
| Any partition expiration strategy or `partition.expiration-time` | Not mapped in the first phase | Keep the Paimon and Fluss lifecycles independent. Configure Fluss retention explicitly instead of deriving it from Paimon. |
| `partition.timestamp-pattern` references a single time column, and `partition.timestamp-formatter` satisfies Fluss format requirements | The time column and format can be mapped | For example, `$dt` and `yyyy-MM-dd` map to `table.auto-partition.key=dt`, `table.auto-partition.time-format=yyyy-MM-dd`, and `table.auto-partition.time-unit=DAY`. Set `table.auto-partition.time-zone` to the time zone used by Paimon's expiration checks. |
| `partition.timestamp-pattern` requires concatenating multiple time columns, or the formatter does not satisfy Fluss format requirements | No direct mapping for the time extraction rule | For example, `$year-$month-$day`. Fluss automatic partition management uses a single time column, with fixed-width time fields ordered from the year down to the configured time unit. |
| `row-tracking.enabled=true` | Rejected in the first phase | A Fluss log table cannot preserve Paimon Row ID update and delete semantics. |
| `data-evolution.enabled=true` | Rejected in the first phase | Updating columns of existing rows cannot be mapped to Fluss log-table append semantics. |
| `deletion-vectors.enabled=true` | Rejected in the first phase | The current Fluss log-table tiering path does not support mutable Paimon append tables. |
| Column `defaultValue` (a schema attribute, not an option) | No direct mapping | There is currently no general mapping for column default values in Fluss. |

`upsert-key`, `rowkind.field`, row tracking, data evolution, and deletion vectors can make an append table mutable. The first implementation phase rejects these definitions instead of silently changing their behavior.

## 2. Primary Key Tables

In the following table, `fields.<column>.agg` is Fluss Flink DDL syntax. It is stored as a column aggregation definition in the Fluss schema. When using the Admin API, construct the corresponding column `AggFunction`.

| Paimon definition / option | Can be mapped? | Corresponding Fluss definition / option and limitations |
| --- | --- | --- |
| Single-column or composite primary key declaration | Yes | Preserve primary key fields and their order, subject to type and NOT NULL constraints. |
| Unpartitioned table, or partitioning by one or more columns | Yes | Preserve the relationship between partition keys and primary keys. Types and partition value encodings must be compatible. |
| `bucket=N`, `N>0`, `bucket-function.type=default`, `bucket-key=k1,k2` | Yes | Use the same `bucket.num=N` and bucket keys, with the corresponding Paimon hash function. The bucket count must satisfy the Fluss cluster limit. |
| `bucket=-1`, i.e. HASH_DYNAMIC / KEY_DYNAMIC | No direct mapping | Fluss has no corresponding dynamic mapping from keys to buckets/partitions. |
| `bucket=-2`, i.e. POSTPONE_MODE | No direct mapping | Fluss has no corresponding table definition for postponed bucketing or selecting bucket counts per partition. |
| `bucket-function.type=mod` | No direct mapping | Fluss currently has no implementation corresponding to Paimon's mod hash. |
| `merge-engine=deduplicate`, without additional ordering rules | Yes | Leave `table.merge-engine` unset to use the default Fluss merger. |
| `merge-engine=first-row` | Yes | Use `table.merge-engine=first_row`. Map `ignore-delete=false` to `table.delete.behavior=disable`, and `true` to `ignore`. |
| `merge-engine=partial-update`, without additional sequence rules, field aggregation, or conditional deletion | Yes | Use `table.merge-engine=aggregation` and set `fields.<column>.agg=last_value_ignore_nulls` for every non-primary-key column. See the deletion options below. |
| `fields.<version-columns>.sequence-group` | No complete mapping | Fluss has no corresponding rules for version comparison and field retraction within sequence groups. |
| `partial-update.remove-record-on-sequence-group` | No direct mapping | Fluss has no definition for deleting an entire row based on conditions for a specified sequence group. |
| `merge-engine=aggregation` | Partially | Use `table.merge-engine=aggregation`. Map aggregation functions, parameters, and deletion/retraction rules for each column. |
| `fields.<column>.aggregate-function` | Partially | Map to `fields.<column>.agg`. See the function values below. |
| `fields.default-aggregate-function` | Yes, for supported functions | Expand it to every non-primary-key column without an explicitly configured function. Fluss has no equivalent global option. |
| `sequence.field=v`, `sequence.field.sort-order=ascending`, `ignore-delete=true` | Yes, for a restricted combination | Requires deduplicate and a single non-null version column of type INT/BIGINT/TIMESTAMP/TIMESTAMP_LTZ. Set `table.merge-engine=versioned`, `table.merge-engine.versioned.ver-column=v`, and `table.delete.behavior=ignore`. Preserve the selection rule for equal versions. |
| Multiple fields in `sequence.field`, or `sequence.field.sort-order=descending` | No direct mapping | Fluss VERSIONED supports a single version column and selects the greater value. |
| Nullable `sequence.field`, or a requirement to perform normal DELETE operations | No general mapping | Comparisons between null and a type's minimum value may differ. Fluss VERSIONED does not support `table.delete.behavior=allow`. |
| `sequence.snapshot-ordering=true` | No direct mapping | Fluss has no corresponding table definition for ordering by Paimon commit snapshot. |
| `ignore-delete=true` | Yes | Use `table.delete.behavior=ignore`. |
| `ignore-delete=false`, with ordinary deduplicate or first-row | Yes | Ordinary deduplicate maps to `table.delete.behavior=allow` (DELETE removes the entire row). First-row maps to `table.delete.behavior=disable` (DELETE raises an error). |
| `partial-update.remove-record-on-delete`, with `ignore-delete=false` | Yes | Map `false` (the default) to `table.delete.behavior=disable`: DELETE raises an error. Map `true` to `table.delete.behavior=allow`: DELETE removes the entire row for the key. |
| `aggregation.remove-record-on-delete=true`, with `ignore-delete=false` | Yes | Use `table.delete.behavior=allow`: DELETE removes the entire row for the key. |
| `fields.<column>.ignore-retract` | No direct per-field mapping | Fluss deletion policies apply to the entire row; there is no definition for arbitrary field-level retraction. |
| Aggregation table with `ignore-update-before=true` | The ignore behavior can be preserved | Paimon ignores UPDATE_BEFORE. The Fluss Flink SQL primary key sink does not accept UPDATE_BEFORE, so no equivalent Fluss table option is required. |
| Aggregation table with `ignore-update-before=false` and `ignore-delete=false`, requiring field retraction | No | Paimon passes UPDATE_BEFORE to aggregation functions to retract the previous contribution. The current Fluss aggregation merger does not support field retraction. |
| Nonempty `rowkind.field` | Cannot map through the table definition alone | No corresponding table property derives the operation type from a data column. |
| `record-level.expire-time` and `record-level.time-field`, with a BIGINT millisecond or TIMESTAMP_LTZ time column | Yes | Map to `table.kv.ttl` and `table.kv.ttl.time-column`, respectively; for example, `7 d` and `event_time`. Neither system expires a row through TTL when the time column is null. Differences in expiration boundaries due to second versus millisecond precision are allowed. |
| `record-level.time-field` is TIMESTAMP | Yes, when the TabletServer uses UTC | Map to `table.kv.ttl.time-column`. Paimon directly uses the time value's internal millisecond count. Fluss converts local time to epoch milliseconds using the TabletServer time zone; using UTC preserves the same interpretation. |
| `record-level.time-field` is INT or BIGINT in seconds | Cannot map through table configuration alone | Fluss does not support INT time columns and always interprets BIGINT as milliseconds. The type or time values must be converted; there is currently no option for seconds. |
| `partition.expiration-strategy=values-time`, `partition.expiration-time` | Yes, allowing rounding to partition retention windows | Map to `table.auto-partition.time-unit` and `table.auto-partition.num-retention`. For example, `7 d` for daily partitions maps to `DAY` and `7`. Round toward longer retention; the time column, format, and time zone must be compatible. |
| `partition.expiration-strategy=update-time`, `partition.expiration-time` | No direct mapping | Fluss automatic partition management has no expiration strategy based on a partition's last update time. |
| `partition.timestamp-pattern`, `partition.timestamp-formatter` | Yes, for a single time column with a compatible format | The same rules as for log tables apply. Map to `table.auto-partition.key`, `table.auto-partition.time-format`, `table.auto-partition.time-unit`, and `table.auto-partition.time-zone`. Concatenating multiple time columns is not supported. |
| Column `defaultValue` (a schema attribute, not an option) | No direct mapping | There is currently no general mapping for column default values in Fluss. |

The basic `partial-update` mapping follows these rules: both systems use a new field value when it is non-null and retain the old value when the new value is null. The first insertion initializes fields from a valid input row. For example, given an old row `(a=1,b=2)` and a new row `(a=3,b=null)`, both produce `(a=3,b=2)`.

The following table maps values of Paimon's `fields.<column>.aggregate-function` to Fluss's `fields.<column>.agg`. All mappings require compatible field types, parameters, ordering, and deletion/retraction rules.

| Paimon option value | Fluss option value | Limitations |
| --- | --- | --- |
| `sum` / `product` | `sum` / `product` | Numeric types supported by both systems. |
| `min` / `max` | `min` / `max` | Comparable types supported by both systems. |
| `last_value` | `last_value` | Preserves null. |
| `last_non_null_value` | `last_value_ignore_nulls` | Ignores null. |
| `first_value` | `first_value` | Preserves the first value, including null. |
| `first_non_null_value` | `first_value_ignore_nulls` | Excludes legacy variants with different semantics. |
| `bool_and` / `bool_or` | `bool_and` / `bool_or` | BOOLEAN. |
| `rbm32` / `rbm64` | `rbm32` / `rbm64` | BYTES with compatible bitmap encoding. |
| `listagg` | Same function name, but no general equivalent mapping | Current Paimon skips empty strings during aggregation, while Fluss directly concatenates values with the delimiter. Fluss also has no corresponding parameter for Paimon's `fields.<column>.distinct=true`. |
| `collect`, `nested_update`, `nested_partial_update`, `merge_map`, `merge_map_with_keytime`, `hll_sketch`, `theta_sketch` | No corresponding values | No direct mapping. |

## 3. Schema Support for Both Table Types

| Paimon definition | Can be mapped? | Corresponding Fluss definition and limitations |
| --- | --- | --- |
| BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE | Yes | Corresponding types. |
| DECIMAL(p,s) | Yes | Preserve precision and scale. |
| CHAR(n), BINARY(n) | Yes | Preserve length, subject to Fluss type constraints. |
| STRING, BYTES | Yes | Correspond to Paimon's unbounded VARCHAR/VARBINARY. |
| Bounded VARCHAR(n), VARBINARY(n) | Yes | Map to STRING/BYTES. Paimon's declared length is maximum type metadata and does not impose a value-length restriction that Fluss must preserve. |
| DATE, TIME(p), TIMESTAMP(p), TIMESTAMP_LTZ(p) | Corresponding types exist | Preserve precision. The actual types and value representations must be supported by both systems. |
| ARRAY, MAP, ROW | Yes, recursively | Every nested type must be mappable. |
| ARRAY, MAP, ROW as primary key or bucket key columns | No | These types do not satisfy Fluss key type constraints. |
| DECIMAL, ARRAY, MAP, ROW as partition columns | No | These types are outside the current set of supported Fluss partition types. |
| BLOB, VARIANT, VECTOR, MULTISET, unknown types | No | No corresponding conversion currently exists. This also applies when these types are nested. |
| Column defaultValue | No direct mapping | A schema attribute with no corresponding Fluss option. |

## 4. Compatibility Limitations

The Paimon versions used to create, alter, and write the source table cannot currently be determined reliably. `TableSchema.version` is a schema format version; it does not identify the source software version. The source version therefore cannot be used to determine the actual meaning of every option.

An unknown option may be a business-specific property or a new feature setting that the current dependency does not recognize. Paimon may also use a previously custom key as a feature option in a future version. These cases cannot be reliably distinguished from key/value pairs alone. Rejecting unknown options can block harmless business properties, while accepting them may overlook new features and change merge, delete, or other behavior after promotion.

Even if every option is known, future versions may change its meaning or default behavior. Conformance to this matrix therefore only means that a table definition satisfies the currently known mapping rules; it does not guarantee safe promotion of tables from arbitrary future Paimon versions.
