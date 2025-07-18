/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.kv.rocksdb;

import com.alibaba.fluss.metrics.Gauge;
import com.alibaba.fluss.metrics.MetricNames;
import com.alibaba.fluss.metrics.groups.MetricGroup;
import com.alibaba.fluss.server.metrics.group.TabletServerMetricGroup;

import org.rocksdb.Cache;
import org.rocksdb.LRUCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

/**
 * Shared block cache metrics collector that collects and reports shared block cache usage metrics
 * at the tablet server level.
 */
public class SharedBlockCacheMetricsCollector implements Closeable {

    private static final Logger LOG =
            LoggerFactory.getLogger(SharedBlockCacheMetricsCollector.class);

    private final MetricGroup tabletServerMetricGroup;
    private final RocksDBSharedResource sharedResource;

    // Cached values for efficient metric access
    private volatile long sharedBlockCacheUsage = 0;
    private volatile long sharedBlockCachePinnedUsage = 0;

    private volatile boolean registered = false;

    public SharedBlockCacheMetricsCollector(
            TabletServerMetricGroup tabletServerMetricGroup, RocksDBSharedResource sharedResource) {
        this.tabletServerMetricGroup = tabletServerMetricGroup;
        this.sharedResource = sharedResource;

        // Register metrics
        registerMetrics();

        // Register this collector with the global manager
        RocksDBMetricsManager.getInstance().registerSharedBlockCacheCollector(this);
        this.registered = true;

        LOG.info("Shared block cache metrics collector started");
    }

    private void registerMetrics() {
        // Register shared block cache usage metrics
        tabletServerMetricGroup.gauge(
                MetricNames.ROCKSDB_SHARED_BLOCK_CACHE_USAGE,
                (Gauge<Long>) () -> sharedBlockCacheUsage);
        tabletServerMetricGroup.gauge(
                MetricNames.ROCKSDB_SHARED_BLOCK_CACHE_PINNED_USAGE,
                (Gauge<Long>) () -> sharedBlockCachePinnedUsage);
    }

    public void updateMetrics() {
        try {
            // Update shared block cache usage metrics
            sharedBlockCacheUsage = getSharedBlockCacheUsage();
            sharedBlockCachePinnedUsage = getSharedBlockCachePinnedUsage();
        } catch (Exception e) {
            LOG.warn("Error updating shared block cache metrics", e);
        }
    }

    private long getSharedBlockCacheUsage() {
        try {
            Cache cache = sharedResource.getSharedBlockCache();
            return cache != null ? ((LRUCache) cache).getUsage() : 0L;
        } catch (Exception e) {
            LOG.debug("Error getting shared block cache usage: {}", e.getMessage());
            return 0;
        }
    }

    private long getSharedBlockCachePinnedUsage() {
        try {
            Cache cache = sharedResource.getSharedBlockCache();
            return cache != null ? ((LRUCache) cache).getPinnedUsage() : 0L;
        } catch (Exception e) {
            LOG.debug("Error getting shared block cache pinned usage: {}", e.getMessage());
            return 0;
        }
    }

    @Override
    public void close() {
        LOG.info("Closing shared block cache metrics collector");

        // Unregister from global manager
        if (registered) {
            RocksDBMetricsManager.getInstance().unregisterSharedBlockCacheCollector(this);
            registered = false;
        }
    }
}
