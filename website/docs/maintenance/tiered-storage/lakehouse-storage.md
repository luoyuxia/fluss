---
title: "Lakehouse Storage"
sidebar_position: 3
---

<!--
 Copyright (c) 2025 Alibaba Group Holding Ltd.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Lakehouse Storage

Lakehouse represents a new, open architecture that combines the best elements of data lakes and data warehouses.
Lakehouse combines data lake scalability and cost-effectiveness with data warehouse reliability and performance.

Fluss leverages the well-known Lakehouse storage solutions like Apache Paimon, Apache Iceberg, Apache Hudi, Delta Lake as
the tiered storage layer. Currently, only Apache Paimon is supported, but more kinds of Lakehouse storage support are on the way.

Fluss's datalake tiering service will tier Fluss's data to the Lakehouse storage continuously. The data in Lakehouse storage can be read both by Fluss's client in a streaming manner and accessed directly
by external systems such as Flink, Spark, StarRocks and others. With data tiered in Lakehouse storage, Fluss
can gain much storage cost reduction and analytics performance improvement.


## Enable Lakehouse Storage

Lakehouse Storage disabled by default, you must enable it manually.

### Lakehouse Storage Cluster Configurations
First, you must configure the lakehouse storage in `server.yaml`. Take Paimon
as an example, you must configure the following configurations:
```yaml
datalake.format: paimon

# the catalog config about Paimon, assuming using Filesystem catalog
datalake.paimon.metastore: filesystem
datalake.paimon.warehouse: /tmp/paimon_data_warehouse
```

Fluss will remove the `datalake.paimon.` prefix and then use the remaining configuration (without the prefix) to create the Paimon catalog.
For example, if you want to configure to use Hive catalog, you can configure like following:
```yaml
datalake.paimon.metastore: hive
datalake.paimon.uri: thrift://<hive-metastore-host-name>:<port>
datalake.paimon.warehouse: hdfs:///path/to/warehouse
```

### Start The Datalake Tiering Service
Then, you must start the datalake tiering service to tier Fluss's data to the lakehouse storage.
To start the datalake tiering service, you must have a Flink cluster running since Fluss currently only supports Flink as a tiering service backend. 
We provide [fluss-flink-tiering-$FLUSS_VERSION$.jar](/downloads) to enable you to submit it to Flink cluster as a Flink job to do the tiering.

The following is the steps you may need to do for your Fluss and lake storage:
#### Prepare jars that Datalake Tiering Service requires
- Put [fluss-flink connector jar](/downloads) into `FLINK_HOME`/lib, you should choose a connector version that matches your Flink version, if you're using Flink 1.20, please use `fluss-flink-1.20`
- If you use [Amazon S3](http://aws.amazon.com/s3/), [Aliyun OSS](https://www.aliyun.com/product/oss) or [HDFS(Hadoop Distributed File System)](https://hadoop.apache.org/docs/stable/) as Fluss's [remote storage](maintenance/tiered-storage/remote-storage.md),
  you should download the corresponding [Fluss filesystem jar](/downloads#filesystem-jars) and also put it into `FLINK_HOME`/lib
- Put [fluss-lake-paimon jar](/downloads) into `FLINK_HOME`/lib, currently only paimon is supported, so you can only choose `fluss-lake-paimon`
- [Download](https://flink.apache.org/downloads/) Pre-bundled Hadoop jar `flink-shaded-hadoop-2-uber-*.jar` and put into `FLINK_HOME`/lib
- Put Paimon's filesystem jar into `FLINK_HOME`/lib, if you use s3 to storage paimon data, please put `paimon-s3`.jar into `FLINK_HOME`/lib
- The other jars that Paimon requires, for example, it you use HiveCatalog, you may still need to put hive related jars


#### Start Datalake Tiering Service
After the Flink Cluster has been started, you can execute the `fluss-flink-tiering-$FLUSS_VERSION$.jar` by using the following command to start datalake tiering service:
```shell
<FLINK_HOME>/bin/flink run /path/to/fluss-flink-tiering-$FLUSS_VERSION$.jar \
    --fluss.bootstrap.servers localhost:9123 \
    --datalake.format paimon \
    --datalake.paimon.metastore filesystem \
    --datalake.paimon.warehouse /tmp/paimon
```

**Note:**
- `fluss.bootstrap.servers` should be the bootstrap server to your Fluss cluster, `--datalake.format`, `--datalake.paimon.metastore`, `--datalake.paimon.warehouse` must be same
  as what you configure in [Cluster Configurations](#lakehouse-storage-cluster-configurations).
- The Flink tiering service is stateless, and you can have multiple tiering services to tiering the tables in FLuss together. The tiering services is coordinated
by Fluss cluster to ensure data tiered to lake is exactly once. That means you can scale it up to multiple or scale it down freely according your workload.


### Enable Lakehouse Storage Per Table
To enable lakehouse storage for a table, the table must be created with the option `'table.datalake.enabled' = 'true'`.

There's an another option `table.datalake.freshness`  to configure the data freshness per-table in lake.
It defines the maximum amount of time that the datalake table's content should lag behind updates to the Fluss table. 
Based on this target freshness, the Fluss tiering service automatically moves data from the Fluss table and updates to the datalake table, so that the data in the datalake table is kept up to date within this target.
The default is 3 minutes, if the data does not need to be as fresh, you can specify a longer target freshness time to reduce costs.