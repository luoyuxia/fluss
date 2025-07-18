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

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.metrics.Gauge;
import com.alibaba.fluss.metrics.MetricNames;
import com.alibaba.fluss.metrics.groups.MetricGroup;
import com.alibaba.fluss.server.metrics.group.BucketMetricGroup;

import org.rocksdb.Cache;
import org.rocksdb.HistogramData;
import org.rocksdb.HistogramType;
import org.rocksdb.MemoryUsageType;
import org.rocksdb.MemoryUtil;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * RocksDB metrics collector that collects and reports RocksDB statistics including cache,
 * compaction, memory, and I/O metrics with table bucket and server information tags.
 */
public class RocksDBMetricsCollector implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBMetricsCollector.class);

    private final RocksDB rocksDB;
    private final Statistics statistics;
    private final MetricGroup bucketMetricGroup;
    private final TableBucket tableBucket;
    private final TablePath tablePath;
    private final String partitionName;
    private final RocksDBResourceContainer resourceContainer;

    private volatile boolean registered = false;

    // Cached values for efficient metric access
    private volatile long blockCacheHitCount = 0;
    private volatile long blockCacheMissCount = 0;
    private volatile long indexBlockCacheHitCount = 0;
    private volatile long indexBlockCacheMissCount = 0;
    private volatile long filterBlockCacheHitCount = 0;
    private volatile long filterBlockCacheMissCount = 0;
    private volatile long dataBlockCacheHitCount = 0;
    private volatile long dataBlockCacheMissCount = 0;
    private volatile long compactionCount = 0;
    private volatile long compactionBytesRead = 0;
    private volatile long compactionBytesWritten = 0;
    private volatile long compactionCpuTimeMicros = 0;
    private volatile long flushCount = 0;
    private volatile long flushBytesWritten = 0;
    private volatile long stallTimeMicros = 0;
    private volatile long bytesRead = 0;
    private volatile long bytesWritten = 0;
    private volatile long numberDbNext = 0;
    private volatile long numberDbPrev = 0;
    private volatile long numberDbSeek = 0;
    private volatile long numberDbSeekFound = 0;
    private volatile long numberKeysRead = 0;
    private volatile long numberKeysWritten = 0;
    private volatile long numberKeysUpdated = 0;
    private volatile long numLiveVersions = 0;
    private volatile long numImmutableMemtables = 0;
    private volatile long numDeletesActiveMemtable = 0;
    private volatile long numDeletesImmutableMemtable = 0;
    private volatile long numEntriesActiveMemtable = 0;
    private volatile long numEntriesImmutableMemtable = 0;
    private volatile long activeMemtableSize = 0;
    private volatile long unflushedMemtableSize = 0;
    private volatile long totalSstFilesSize = 0;
    private volatile long liveSstFilesSize = 0;
    private volatile long sizeAllMemtables = 0;
    private volatile long blockCacheAddCount = 0;
    private volatile long memtableMemoryUsage = 0;
    private volatile long blockCacheMemoryUsage = 0;
    private volatile long tableReadersMemoryUsage = 0;
    private volatile long totalMemoryUsage = 0;
    private volatile long blockCacheUsage = 0;
    private volatile long blockCachePinnedUsage = 0;

    // Level SST files metrics
    private volatile long numFilesAtLevel0 = 0;
    private volatile long numFilesAtLevel1 = 0;
    private volatile long numFilesAtLevel2 = 0;
    private volatile long numFilesAtLevel3 = 0;
    private volatile long numFilesAtLevel4 = 0;
    private volatile long numFilesAtLevel5 = 0;
    private volatile long numFilesAtLevel6 = 0;

    // Latency metrics
    private volatile long getMicros = 0;
    private volatile long writeStallMicros = 0;
    private volatile long writeDoneBySelf = 0;
    private volatile long writeDoneByOther = 0;
    private volatile long writeWithWal = 0;
    private volatile long writeWithPrepare = 0;

    // Compression and compaction pressure metrics
    private volatile long compactionPending = 0;
    private volatile long compactionQueueSize = 0;
    private volatile long flushPending = 0;
    private volatile long flushQueueSize = 0;
    private volatile long numRunningCompactions = 0;
    private volatile long numRunningFlushes = 0;

    // Write amplification metrics
    private volatile long writeAmplification = 0;
    private volatile long readAmplification = 0;
    private volatile long compactReadBytes = 0;
    private volatile long compactWriteBytes = 0;

    private volatile long bloomFilterUseful = 0;
    private volatile long bloomFilterFullPositive = 0;
    private volatile long bloomFilterFullTruePositive = 0;

    private volatile long backgroundErrors = 0;

    private volatile long numSstFiles = 0;
    private volatile long numDeletesObtained = 0;
    private volatile long numKeysExist = 0;
    private volatile long numKeysNotFound = 0;

    public RocksDBMetricsCollector(
            RocksDB rocksDB,
            Statistics statistics,
            BucketMetricGroup bucketMetricGroup,
            TableBucket tableBucket,
            TablePath tablePath,
            @Nullable String partitionName,
            RocksDBResourceContainer resourceContainer) {
        this.rocksDB = rocksDB;
        this.statistics = statistics;
        this.tableBucket = tableBucket;
        this.tablePath = tablePath;
        this.partitionName = partitionName;
        this.resourceContainer = resourceContainer;

        // Create RocksDB metrics group under the bucket metrics
        this.bucketMetricGroup = bucketMetricGroup;

        // Register all metrics
        registerMetrics();

        // Register this reporter with the global collector
        RocksDBMetricsManager.getInstance().registerCollector(this);
        this.registered = true;

        LOG.info(
                "RocksDB metrics collector started for table {} bucket {}",
                tablePath,
                tableBucket.getBucket());
    }

    private void registerMetrics() {
        // Cache metrics - 统一使用缓存变量
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_BLOCK_CACHE_HIT_COUNT, (Gauge<Long>) () -> blockCacheHitCount);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_BLOCK_CACHE_MISS_COUNT,
                (Gauge<Long>) () -> blockCacheMissCount);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_INDEX_BLOCK_CACHE_HIT_COUNT,
                (Gauge<Long>) () -> indexBlockCacheHitCount);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_INDEX_BLOCK_CACHE_MISS_COUNT,
                (Gauge<Long>) () -> indexBlockCacheMissCount);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_FILTER_BLOCK_CACHE_HIT_COUNT,
                (Gauge<Long>) () -> filterBlockCacheHitCount);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_FILTER_BLOCK_CACHE_MISS_COUNT,
                (Gauge<Long>) () -> filterBlockCacheMissCount);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_DATA_BLOCK_CACHE_HIT_COUNT,
                (Gauge<Long>) () -> dataBlockCacheHitCount);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_DATA_BLOCK_CACHE_MISS_COUNT,
                (Gauge<Long>) () -> dataBlockCacheMissCount);

        // Compaction metrics
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_COMPACTION_COUNT, (Gauge<Long>) () -> compactionCount);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_COMPACTION_BYTES_READ, (Gauge<Long>) () -> compactionBytesRead);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_COMPACTION_BYTES_WRITTEN,
                (Gauge<Long>) () -> compactionBytesWritten);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_COMPACTION_CPU_TIME_MICROS,
                (Gauge<Long>) () -> compactionCpuTimeMicros);
        bucketMetricGroup.gauge(MetricNames.ROCKSDB_FLUSH_COUNT, (Gauge<Long>) () -> flushCount);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_FLUSH_BYTES_WRITTEN, (Gauge<Long>) () -> flushBytesWritten);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_STALL_TIME_MICROS, (Gauge<Long>) () -> stallTimeMicros);

        // Memory metrics - 统一使用缓存变量
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_MEMTABLE_MEMORY_USAGE, (Gauge<Long>) () -> memtableMemoryUsage);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_BLOCK_CACHE_MEMORY_USAGE,
                (Gauge<Long>) () -> blockCacheMemoryUsage);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_TABLE_READERS_MEMORY_USAGE,
                (Gauge<Long>) () -> tableReadersMemoryUsage);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_TOTAL_MEMORY_USAGE, (Gauge<Long>) () -> totalMemoryUsage);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_ACTIVE_MEMTABLE_SIZE, (Gauge<Long>) () -> activeMemtableSize);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_UNFLUSHED_MEMTABLE_SIZE,
                (Gauge<Long>) () -> unflushedMemtableSize);

        // BlockCache usage指标 - 统一使用缓存变量
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_BLOCK_CACHE_USAGE, (Gauge<Long>) () -> blockCacheUsage);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_BLOCK_CACHE_PINNED_USAGE,
                (Gauge<Long>) () -> blockCachePinnedUsage);

        // I/O metrics
        bucketMetricGroup.gauge(MetricNames.ROCKSDB_BYTES_READ, (Gauge<Long>) () -> bytesRead);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_BYTES_WRITTEN, (Gauge<Long>) () -> bytesWritten);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_NUMBER_DB_NEXT, (Gauge<Long>) () -> numberDbNext);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_NUMBER_DB_PREV, (Gauge<Long>) () -> numberDbPrev);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_NUMBER_DB_SEEK, (Gauge<Long>) () -> numberDbSeek);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_NUMBER_DB_SEEK_FOUND, (Gauge<Long>) () -> numberDbSeekFound);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_NUMBER_KEYS_READ, (Gauge<Long>) () -> numberKeysRead);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_NUMBER_KEYS_WRITTEN, (Gauge<Long>) () -> numberKeysWritten);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_NUMBER_KEYS_UPDATED, (Gauge<Long>) () -> numberKeysUpdated);

        // SST files metrics
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_NUM_LIVE_VERSIONS, (Gauge<Long>) () -> numLiveVersions);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_NUM_IMMUTABLE_MEMTABLES,
                (Gauge<Long>) () -> numImmutableMemtables);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_NUM_DELETES_ACTIVE_MEMTABLE,
                (Gauge<Long>) () -> numDeletesActiveMemtable);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_NUM_DELETES_IMMUTABLE_MEMTABLE,
                (Gauge<Long>) () -> numDeletesImmutableMemtable);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_NUM_ENTRIES_ACTIVE_MEMTABLE,
                (Gauge<Long>) () -> numEntriesActiveMemtable);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_NUM_ENTRIES_IMMUTABLE_MEMTABLE,
                (Gauge<Long>) () -> numEntriesImmutableMemtable);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_TOTAL_SST_FILES_SIZE, (Gauge<Long>) () -> totalSstFilesSize);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_LIVE_SST_FILES_SIZE, (Gauge<Long>) () -> liveSstFilesSize);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_SIZE_ALL_MEMTABLES, (Gauge<Long>) () -> sizeAllMemtables);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_BLOCK_CACHE_ADD_COUNT, (Gauge<Long>) () -> blockCacheAddCount);

        // Level SST files metrics
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_NUM_FILES_AT_LEVEL_0, (Gauge<Long>) () -> numFilesAtLevel0);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_NUM_FILES_AT_LEVEL_1, (Gauge<Long>) () -> numFilesAtLevel1);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_NUM_FILES_AT_LEVEL_2, (Gauge<Long>) () -> numFilesAtLevel2);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_NUM_FILES_AT_LEVEL_3, (Gauge<Long>) () -> numFilesAtLevel3);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_NUM_FILES_AT_LEVEL_4, (Gauge<Long>) () -> numFilesAtLevel4);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_NUM_FILES_AT_LEVEL_5, (Gauge<Long>) () -> numFilesAtLevel5);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_NUM_FILES_AT_LEVEL_6, (Gauge<Long>) () -> numFilesAtLevel6);

        // Latency metrics
        bucketMetricGroup.gauge(MetricNames.ROCKSDB_GET_MICROS, (Gauge<Long>) () -> getMicros);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_WRITE_STALL_MICROS, (Gauge<Long>) () -> writeStallMicros);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_WRITE_DONE_BY_SELF, (Gauge<Long>) () -> writeDoneBySelf);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_WRITE_DONE_BY_OTHER, (Gauge<Long>) () -> writeDoneByOther);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_WRITE_WITH_WAL, (Gauge<Long>) () -> writeWithWal);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_WRITE_WITH_PREPARE, (Gauge<Long>) () -> writeWithPrepare);

        // Compression and compaction pressure metrics
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_COMPACTION_PENDING, (Gauge<Long>) () -> compactionPending);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_COMPACTION_QUEUE_SIZE, (Gauge<Long>) () -> compactionQueueSize);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_FLUSH_PENDING, (Gauge<Long>) () -> flushPending);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_FLUSH_QUEUE_SIZE, (Gauge<Long>) () -> flushQueueSize);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_NUM_RUNNING_COMPACTIONS,
                (Gauge<Long>) () -> numRunningCompactions);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_NUM_RUNNING_FLUSHES, (Gauge<Long>) () -> numRunningFlushes);

        // Write amplification metrics
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_WRITE_AMPLIFICATION, (Gauge<Long>) () -> writeAmplification);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_READ_AMPLIFICATION, (Gauge<Long>) () -> readAmplification);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_COMPACT_READ_BYTES, (Gauge<Long>) () -> compactReadBytes);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_COMPACT_WRITE_BYTES, (Gauge<Long>) () -> compactWriteBytes);

        // Bloom filter metrics
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_BLOOM_FILTER_USEFUL, (Gauge<Long>) () -> bloomFilterUseful);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_BLOOM_FILTER_FULL_POSITIVE,
                (Gauge<Long>) () -> bloomFilterFullPositive);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_BLOOM_FILTER_FULL_TRUE_POSITIVE,
                (Gauge<Long>) () -> bloomFilterFullTruePositive);

        // Background job metrics
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_BACKGROUND_ERRORS, (Gauge<Long>) () -> backgroundErrors);
        // SST file metrics
        bucketMetricGroup.gauge(MetricNames.ROCKSDB_NUM_SST_FILES, (Gauge<Long>) () -> numSstFiles);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_NUM_DELETES_OBTAINED, (Gauge<Long>) () -> numDeletesObtained);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_NUM_KEYS_EXIST, (Gauge<Long>) () -> numKeysExist);
        bucketMetricGroup.gauge(
                MetricNames.ROCKSDB_NUM_KEYS_NOT_FOUND, (Gauge<Long>) () -> numKeysNotFound);
    }

    public void updateMetrics() {
        try {
            // Update cache metrics
            blockCacheHitCount = getTickerValue(TickerType.BLOCK_CACHE_HIT);
            blockCacheMissCount = getTickerValue(TickerType.BLOCK_CACHE_MISS);
            indexBlockCacheHitCount = getTickerValue(TickerType.BLOCK_CACHE_INDEX_HIT);
            indexBlockCacheMissCount = getTickerValue(TickerType.BLOCK_CACHE_INDEX_MISS);
            filterBlockCacheHitCount = getTickerValue(TickerType.BLOCK_CACHE_FILTER_HIT);
            filterBlockCacheMissCount = getTickerValue(TickerType.BLOCK_CACHE_FILTER_MISS);
            dataBlockCacheHitCount = getTickerValue(TickerType.BLOCK_CACHE_DATA_HIT);
            dataBlockCacheMissCount = getTickerValue(TickerType.BLOCK_CACHE_DATA_MISS);

            // Update compaction metrics
            compactionCount =
                    getTickerValue(TickerType.COMPACT_READ_BYTES) > 0
                            ? 1
                            : 0; // Simplified since exact count may not be available
            compactionBytesRead = getTickerValue(TickerType.COMPACT_READ_BYTES);
            compactionBytesWritten = getTickerValue(TickerType.COMPACT_WRITE_BYTES);
            compactionCpuTimeMicros = getHistogramValue(HistogramType.COMPACTION_TIME);
            flushCount = getTickerValue(TickerType.FLUSH_WRITE_BYTES) > 0 ? 1 : 0;
            flushBytesWritten = getTickerValue(TickerType.FLUSH_WRITE_BYTES);
            stallTimeMicros = getTickerValue(TickerType.STALL_MICROS);

            // Update I/O metrics
            bytesRead = getTickerValue(TickerType.BYTES_READ);
            bytesWritten = getTickerValue(TickerType.BYTES_WRITTEN);
            numberDbNext = getTickerValue(TickerType.NUMBER_DB_NEXT);
            numberDbPrev = getTickerValue(TickerType.NUMBER_DB_PREV);
            numberDbSeek = getTickerValue(TickerType.NUMBER_DB_SEEK);
            numberDbSeekFound = getTickerValue(TickerType.NUMBER_DB_SEEK_FOUND);
            numberKeysRead = getTickerValue(TickerType.NUMBER_KEYS_READ);
            numberKeysWritten = getTickerValue(TickerType.NUMBER_KEYS_WRITTEN);
            numberKeysUpdated = getTickerValue(TickerType.NUMBER_KEYS_UPDATED);

            // Update SST files metrics - using property-based approach for more accurate values
            numLiveVersions = getPropertyValueAsLong("rocksdb.num-live-versions");
            numImmutableMemtables = getPropertyValueAsLong("rocksdb.num-immutable-mem-table");
            numDeletesActiveMemtable =
                    getPropertyValueAsLong("rocksdb.num-deletes-active-mem-table");
            numDeletesImmutableMemtable =
                    getPropertyValueAsLong("rocksdb.num-deletes-immutable-mem-table");
            numEntriesActiveMemtable =
                    getPropertyValueAsLong("rocksdb.num-entries-active-mem-table");
            numEntriesImmutableMemtable =
                    getPropertyValueAsLong("rocksdb.num-entries-immutable-mem-table");

            // Update property-based metrics
            activeMemtableSize = getPropertyValueAsLong("rocksdb.cur-size-active-mem-table");
            unflushedMemtableSize = getPropertyValueAsLong("rocksdb.cur-size-all-mem-tables");
            totalSstFilesSize = getPropertyValueAsLong("rocksdb.total-sst-files-size");
            liveSstFilesSize = getPropertyValueAsLong("rocksdb.live-sst-files-size");
            sizeAllMemtables = getPropertyValueAsLong("rocksdb.size-all-mem-tables");

            blockCacheAddCount = getTickerValue(TickerType.BLOCK_CACHE_ADD);

            memtableMemoryUsage = getMemoryUsage(MemoryUsageType.kMemTableTotal);
            blockCacheMemoryUsage = getMemoryUsage(MemoryUsageType.kCacheTotal);
            tableReadersMemoryUsage = getMemoryUsage(MemoryUsageType.kTableReadersTotal);
            totalMemoryUsage = getTotalMemoryUsage();

            // Update block cache usage metrics
            blockCacheUsage = getBlockCacheUsage();
            blockCachePinnedUsage = getBlockCachePinnedUsage();

            // Update level SST files metrics
            numFilesAtLevel0 = getPropertyValueAsLong("rocksdb.num-files-at-level0");
            numFilesAtLevel1 = getPropertyValueAsLong("rocksdb.num-files-at-level1");
            numFilesAtLevel2 = getPropertyValueAsLong("rocksdb.num-files-at-level2");
            numFilesAtLevel3 = getPropertyValueAsLong("rocksdb.num-files-at-level3");
            numFilesAtLevel4 = getPropertyValueAsLong("rocksdb.num-files-at-level4");
            numFilesAtLevel5 = getPropertyValueAsLong("rocksdb.num-files-at-level5");
            numFilesAtLevel6 = getPropertyValueAsLong("rocksdb.num-files-at-level6");

            // Update latency metrics
            // Note: GET_MICROS may not exist in all RocksDB versions, using 0 as default
            getMicros = 0;
            writeStallMicros = getTickerValue(TickerType.STALL_MICROS);
            writeDoneBySelf = getTickerValue(TickerType.WRITE_DONE_BY_SELF);
            writeDoneByOther = getTickerValue(TickerType.WRITE_DONE_BY_OTHER);
            writeWithWal = getTickerValue(TickerType.WRITE_WITH_WAL);
            // Note: WRITE_WITH_PREPARE may not exist in all RocksDB versions, using 0 as default
            writeWithPrepare = 0;

            // Update compression and compaction pressure metrics
            compactionPending = getPropertyValueAsLong("rocksdb.compaction-pending");
            compactionQueueSize = getPropertyValueAsLong("rocksdb.compaction-queue-size");
            flushPending = getPropertyValueAsLong("rocksdb.flush-pending");
            flushQueueSize = getPropertyValueAsLong("rocksdb.flush-queue-size");
            numRunningCompactions = getPropertyValueAsLong("rocksdb.num-running-compactions");
            numRunningFlushes = getPropertyValueAsLong("rocksdb.num-running-flushes");

            // Update write amplification metrics
            writeAmplification = getPropertyValueAsLong("rocksdb.write-amplification");
            readAmplification = getPropertyValueAsLong("rocksdb.read-amplification");
            compactReadBytes = getPropertyValueAsLong("rocksdb.compact-read-bytes");
            compactWriteBytes = getPropertyValueAsLong("rocksdb.compact-write-bytes");

            // Update Bloom filter metrics
            bloomFilterUseful = getPropertyValueAsLong("rocksdb.bloom-filter-useful");
            bloomFilterFullPositive = getPropertyValueAsLong("rocksdb.bloom-filter-full-positive");
            bloomFilterFullTruePositive =
                    getPropertyValueAsLong("rocksdb.bloom-filter-full-true-positive");

            // Update Background job metrics
            backgroundErrors = getPropertyValueAsLong("rocksdb.background-errors");
            // Update SST file metrics
            numSstFiles = getPropertyValueAsLong("rocksdb.num-sst-files");
            numDeletesObtained = getPropertyValueAsLong("rocksdb.num-deletes-obtained");
            numKeysExist = getPropertyValueAsLong("rocksdb.num-keys-exist");
            numKeysNotFound = getPropertyValueAsLong("rocksdb.num-keys-not-found");

        } catch (Exception e) {
            LOG.warn(
                    "Error updating RocksDB metrics for table {} bucket {}",
                    tablePath,
                    tableBucket.getBucket(),
                    e);
        }
    }

    private long getTickerValue(TickerType tickerType) {
        try {
            if (statistics != null) {
                return statistics.getTickerCount(tickerType);
            }
            return 0;
        } catch (Exception e) {
            LOG.debug("Error getting ticker value for {}: {}", tickerType, e.getMessage());
            return 0;
        }
    }

    private long getHistogramValue(HistogramType histogramType) {
        try {
            if (statistics != null) {
                HistogramData histogramData = statistics.getHistogramData(histogramType);
                return histogramData != null ? (long) histogramData.getAverage() : 0;
            }
            return 0;
        } catch (Exception e) {
            LOG.debug("Error getting histogram value for {}: {}", histogramType, e.getMessage());
            return 0;
        }
    }

    private long getPropertyValueAsLong(String property) {
        try {
            String value = rocksDB.getProperty(property);
            return value != null ? Long.parseLong(value) : 0;
        } catch (NumberFormatException | RocksDBException e) {
            LOG.debug("Error getting property value for {}: {}", property, e.getMessage());
            return 0;
        }
    }

    private long getMemoryUsage(MemoryUsageType memoryUsageType) {
        try {
            Set<Cache> caches = new HashSet<>();
            Cache blockCache = resourceContainer.getBlockCache();
            if (blockCache != null) {
                caches.add(blockCache);
            }

            Map<MemoryUsageType, Long> memoryUsage =
                    MemoryUtil.getApproximateMemoryUsageByType(
                            Collections.singletonList(rocksDB), caches);
            return memoryUsage.getOrDefault(memoryUsageType, 0L);
        } catch (Exception e) {
            LOG.debug("Error getting memory usage for {}: {}", memoryUsageType, e.getMessage());
            return 0;
        }
    }

    private long getTotalMemoryUsage() {
        try {
            Set<Cache> caches = new HashSet<>();
            Cache blockCache = resourceContainer.getBlockCache();
            if (blockCache != null) {
                caches.add(blockCache);
            }

            Map<MemoryUsageType, Long> memoryUsage =
                    MemoryUtil.getApproximateMemoryUsageByType(
                            Collections.singletonList(rocksDB), caches);
            return memoryUsage.values().stream().mapToLong(Long::longValue).sum();
        } catch (Exception e) {
            LOG.debug("Error getting total memory usage: {}", e.getMessage());
            return 0;
        }
    }

    private long getBlockCacheUsage() {
        try {
            Cache cache = resourceContainer.getBlockCache();
            return cache != null ? ((org.rocksdb.LRUCache) cache).getUsage() : 0L;
        } catch (Exception e) {
            LOG.debug("Error getting block cache usage: {}", e.getMessage());
            return 0;
        }
    }

    private long getBlockCachePinnedUsage() {
        try {
            Cache cache = resourceContainer.getBlockCache();
            return cache != null ? ((org.rocksdb.LRUCache) cache).getPinnedUsage() : 0L;
        } catch (Exception e) {
            LOG.debug("Error getting block cache pinned usage: {}", e.getMessage());
            return 0;
        }
    }

    @Override
    public void close() {
        LOG.info(
                "Closing RocksDB metrics collector for table {} bucket {}.",
                tablePath,
                tableBucket.getBucket());

        // Unregister from global collector
        if (registered) {
            RocksDBMetricsManager.getInstance().unregisterCollector(this);
            registered = false;
        }
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public TablePath getTablePath() {
        return tablePath;
    }

    public String getPartitionName() {
        return partitionName;
    }
}
