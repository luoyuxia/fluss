---
title: Real-Time Analytics with Flink
sidebar_position: 1
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import PaimonContent from './_flink-paimon-tab.md';
import IcebergContent from './_flink-iceberg-tab.md';

# Real-Time Analytics With Flink

This guide will get you up and running with Apache Flink to do real-time analytics, covering some powerful features of Fluss,
including integrating with different lake table formats.
The guide is derived from [TPC-H](https://www.tpc.org/tpch/) **Q5**.

For more information on working with Flink, refer to the [Apache Flink Engine](engine-flink/getting-started.md) section.

## Choose Your Lake Table Format

<Tabs>
  <TabItem value="paimon" label="🚀 Paimon Integration" default>
    <PaimonContent />
  </TabItem>
  <TabItem value="iceberg" label="🧊 Iceberg Integration">
    <IcebergContent />
  </TabItem>
</Tabs>

## Clean up
After finishing the tutorial, run `exit` to exit Flink SQL CLI Container and then run 
```shell
docker compose down -v
```
to stop all containers.

## Learn more
Now that you're up and running with Fluss and Flink, check out the [Apache Flink Engine](engine-flink/getting-started.md) docs to learn more features with Flink or [this guide](/maintenance/observability/quickstart.md) to learn how to set up an observability stack for Fluss and Flink.