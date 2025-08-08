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

package com.alibaba.fluss.utils;

import com.alibaba.fluss.utils.concurrent.ThreadUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Process exit monitor that captures comprehensive information when the process is shutting down.
 * This class provides detailed logging of system state, memory usage, thread information, and error
 * context during process termination.
 */
public class ProcessExitMonitor {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessExitMonitor.class);

    private static final ProcessExitMonitor INSTANCE = new ProcessExitMonitor();
    private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);
    private static final AtomicLong START_TIME = new AtomicLong(System.currentTimeMillis());

    private final Thread shutdownHook;
    private final Thread.UncaughtExceptionHandler originalHandler;

    private ProcessExitMonitor() {
        this.shutdownHook = new Thread(this::onShutdown, "ProcessExitMonitor-ShutdownHook");
        this.originalHandler = Thread.getDefaultUncaughtExceptionHandler();
    }

    /**
     * Initialize the process exit monitor. This should be called early in the application startup.
     */
    public static void initialize() {
        if (INITIALIZED.compareAndSet(false, true)) {
            INSTANCE.setupMonitoring();
            LOG.info("Process exit monitor initialized successfully");
        }
    }

    /** Cleanup the process exit monitor. This should be called during normal shutdown. */
    public static void cleanup() {
        if (INITIALIZED.get()) {
            INSTANCE.cleanupMonitoring();
            LOG.info("Process exit monitor cleaned up");
        }
    }

    private void setupMonitoring() {
        // Register shutdown hook
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        // Set custom uncaught exception handler
        Thread.setDefaultUncaughtExceptionHandler(new EnhancedUncaughtExceptionHandler());

        LOG.info(
                "Process exit monitoring setup completed. Process started at: {}",
                new java.util.Date(START_TIME.get()));
    }

    private void cleanupMonitoring() {
        try {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (IllegalStateException e) {
            // JVM is already shutting down, ignore
            LOG.debug("Cannot remove shutdown hook, JVM is already shutting down");
        }

        // Restore original uncaught exception handler
        Thread.setDefaultUncaughtExceptionHandler(originalHandler);
    }

    private void onShutdown() {
        LOG.error("=== PROCESS SHUTDOWN INITIATED ===");
        LOG.error("Process uptime: {} ms", System.currentTimeMillis() - START_TIME.get());

        try {
            logSystemState();
            logMemoryState();
            logThreadState();
            logJvmState();
        } catch (Exception e) {
            LOG.error("Error during shutdown logging", e);
        }

        LOG.error("=== PROCESS SHUTDOWN COMPLETE ===");
    }

    private void logSystemState() {
        try {
            OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
            LOG.error("=== SYSTEM STATE ===");
            LOG.error("OS: {} {}", osBean.getName(), osBean.getVersion());
            LOG.error("Architecture: {}", osBean.getArch());
            LOG.error("Available processors: {}", osBean.getAvailableProcessors());
            LOG.error("System load average: {}", osBean.getSystemLoadAverage());

            if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
                com.sun.management.OperatingSystemMXBean sunOsBean =
                        (com.sun.management.OperatingSystemMXBean) osBean;
                LOG.error(
                        "Total physical memory: {} MB",
                        sunOsBean.getTotalPhysicalMemorySize() / (1024 * 1024));
                LOG.error(
                        "Free physical memory: {} MB",
                        sunOsBean.getFreePhysicalMemorySize() / (1024 * 1024));
                LOG.error(
                        "Committed virtual memory: {} MB",
                        sunOsBean.getCommittedVirtualMemorySize() / (1024 * 1024));
            }
        } catch (Exception e) {
            LOG.error("Failed to log system state", e);
        }
    }

    private void logMemoryState() {
        try {
            MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
            MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
            MemoryUsage nonHeapUsage = memoryBean.getNonHeapMemoryUsage();

            LOG.error("=== MEMORY STATE ===");
            LOG.error(
                    "Heap memory - Used: {} MB, Committed: {} MB, Max: {} MB",
                    heapUsage.getUsed() / (1024 * 1024),
                    heapUsage.getCommitted() / (1024 * 1024),
                    heapUsage.getMax() / (1024 * 1024));
            LOG.error(
                    "Non-heap memory - Used: {} MB, Committed: {} MB, Max: {} MB",
                    nonHeapUsage.getUsed() / (1024 * 1024),
                    nonHeapUsage.getCommitted() / (1024 * 1024),
                    nonHeapUsage.getMax() / (1024 * 1024));
        } catch (Exception e) {
            LOG.error("Failed to log memory state", e);
        }
    }

    private void logThreadState() {
        try {
            ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
            LOG.error("=== THREAD STATE ===");
            LOG.error("Total threads: {}", threadBean.getThreadCount());
            LOG.error("Peak thread count: {}", threadBean.getPeakThreadCount());
            LOG.error("Daemon threads: {}", threadBean.getDaemonThreadCount());
            LOG.error("Started threads: {}", threadBean.getTotalStartedThreadCount());

            // Log thread dump
            ThreadUtils.errorLogThreadDump(LOG);
        } catch (Exception e) {
            LOG.error("Failed to log thread state", e);
        }
    }

    private void logJvmState() {
        try {
            LOG.error("=== JVM STATE ===");
            LOG.error("JVM version: {}", System.getProperty("java.version"));
            LOG.error("JVM vendor: {}", System.getProperty("java.vendor"));
            LOG.error("JVM name: {}", System.getProperty("java.vm.name"));
            LOG.error("JVM arguments: {}", System.getProperty("sun.java.command"));

            Runtime runtime = Runtime.getRuntime();
            LOG.error("Available processors: {}", runtime.availableProcessors());
            LOG.error("Total memory: {} MB", runtime.totalMemory() / (1024 * 1024));
            LOG.error("Free memory: {} MB", runtime.freeMemory() / (1024 * 1024));
            LOG.error("Max memory: {} MB", runtime.maxMemory() / (1024 * 1024));
        } catch (Exception e) {
            LOG.error("Failed to log JVM state", e);
        }
    }

    /** Enhanced uncaught exception handler that provides detailed error context. */
    private static class EnhancedUncaughtExceptionHandler
            implements Thread.UncaughtExceptionHandler {

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            LOG.error("=== UNCAUGHT EXCEPTION DETECTED ===");
            LOG.error("Thread: {} (ID: {})", t.getName(), t.getId());
            LOG.error("Thread state: {}", t.getState());
            LOG.error("Thread priority: {}", t.getPriority());
            LOG.error("Thread daemon: {}", t.isDaemon());
            LOG.error("Exception: {}", e.getMessage(), e);

            // Log thread dump
            ThreadUtils.errorLogThreadDump(LOG);

            // Call original handler if available
            if (INSTANCE.originalHandler != null) {
                INSTANCE.originalHandler.uncaughtException(t, e);
            }
        }
    }

    /**
     * Log a critical error with enhanced context information. This method should be called when a
     * critical error occurs that might lead to process termination.
     */
    public static void logCriticalError(String context, Throwable error) {
        LOG.error("=== CRITICAL ERROR DETECTED ===");
        LOG.error("Context: {}", context);
        LOG.error("Error: {}", error.getMessage(), error);
        LOG.error("Process uptime: {} ms", System.currentTimeMillis() - START_TIME.get());

        try {
            // Log current system state
            OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
            MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
            MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();

            LOG.error(
                    "System load: {}, Heap used: {} MB",
                    osBean.getSystemLoadAverage(),
                    heapUsage.getUsed() / (1024 * 1024));
        } catch (Exception e) {
            LOG.error("Failed to log additional context", e);
        }
    }

    /** Log a warning with process context information. */
    public static void logProcessWarning(String message, Object... args) {
        LOG.warn("=== PROCESS WARNING ===");
        LOG.warn(message, args);
        LOG.warn("Process uptime: {} ms", System.currentTimeMillis() - START_TIME.get());
    }
}
