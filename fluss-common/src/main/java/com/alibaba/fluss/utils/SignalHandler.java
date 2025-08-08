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

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Signal handler that captures and logs system signals sent to the process. This class provides
 * detailed logging of signal events and attempts to identify signal sources.
 */
public class SignalHandler {

    private static final Logger LOG = LoggerFactory.getLogger(SignalHandler.class);

    private static final SignalHandler INSTANCE = new SignalHandler();
    private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);
    private static final AtomicLong START_TIME = new AtomicLong(System.currentTimeMillis());

    private final sun.misc.SignalHandler originalHandler;
    private final Thread signalLoggingThread;
    private volatile boolean shutdownRequested = false;

    private SignalHandler() {
        this.originalHandler =
                sun.misc.Signal.handle(new sun.misc.Signal("TERM"), this::handleSignal);
        this.signalLoggingThread =
                new Thread(this::signalLoggingLoop, "SignalHandler-LoggingThread");
        this.signalLoggingThread.setDaemon(true);
    }

    /** Initialize the signal handler. This should be called early in the application startup. */
    public static void initialize() {
        if (INITIALIZED.compareAndSet(false, true)) {
            INSTANCE.setupSignalHandling();
            LOG.info("Signal handler initialized successfully");
        }
    }

    /** Cleanup the signal handler. This should be called during normal shutdown. */
    public static void cleanup() {
        if (INITIALIZED.get()) {
            INSTANCE.cleanupSignalHandling();
            LOG.info("Signal handler cleaned up");
        }
    }

    private void setupSignalHandling() {
        // Register handlers for common signals
        registerSignalHandler("TERM", "SIGTERM - Termination request");
        registerSignalHandler("INT", "SIGINT - Interrupt from keyboard");
        registerSignalHandler("HUP", "SIGHUP - Hangup detected on controlling terminal");
        registerSignalHandler("USR1", "SIGUSR1 - User defined signal 1");
        registerSignalHandler("USR2", "SIGUSR2 - User defined signal 2");

        // Start signal logging thread
        signalLoggingThread.start();

        LOG.info("Signal handling setup completed. Monitoring signals: TERM, INT, HUP, USR1, USR2");
    }

    private void cleanupSignalHandling() {
        shutdownRequested = true;
        signalLoggingThread.interrupt();

        try {
            signalLoggingThread.join(5000); // Wait up to 5 seconds
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while waiting for signal logging thread to stop");
            Thread.currentThread().interrupt();
        }
    }

    private void registerSignalHandler(String signalName, String description) {
        try {
            sun.misc.Signal signal = new sun.misc.Signal(signalName);
            sun.misc.Signal.handle(signal, this::handleSignal);
            LOG.debug("Registered signal handler for {}: {}", signalName, description);
        } catch (IllegalArgumentException e) {
            LOG.debug("Signal {} not available on this platform", signalName);
        } catch (Exception e) {
            LOG.warn("Failed to register signal handler for {}", signalName, e);
        }
    }

    private void handleSignal(sun.misc.Signal signal) {
        LOG.error("=== SIGNAL RECEIVED ===");
        LOG.error("Signal: {} ({})", signal.getName(), signal.getNumber());
        LOG.error("Process uptime: {} ms", System.currentTimeMillis() - START_TIME.get());
        LOG.error("Process ID: {}", getProcessId());

        try {
            logSignalContext(signal);
            logProcessState();
            attemptSignalSourceIdentification(signal);
        } catch (Exception e) {
            LOG.error("Error during signal handling", e);
        }

        // Call original handler if available
        if (originalHandler != null) {
            originalHandler.handle(signal);
        }

        LOG.error("=== SIGNAL HANDLING COMPLETE ===");
    }

    private void logSignalContext(sun.misc.Signal signal) {
        LOG.error("=== SIGNAL CONTEXT ===");
        LOG.error("Signal name: {}", signal.getName());
        LOG.error("Signal number: {}", signal.getNumber());
        LOG.error(
                "Current thread: {} (ID: {})",
                Thread.currentThread().getName(),
                Thread.currentThread().getId());
        LOG.error("Thread state: {}", Thread.currentThread().getState());
        LOG.error("Thread priority: {}", Thread.currentThread().getPriority());
        LOG.error("Thread daemon: {}", Thread.currentThread().isDaemon());

        // Log stack trace of current thread
        LOG.error("Current thread stack trace:");
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        for (StackTraceElement element : stackTrace) {
            LOG.error("  at {}", element);
        }
    }

    private void logProcessState() {
        try {
            ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
            Runtime runtime = Runtime.getRuntime();

            LOG.error("=== PROCESS STATE ===");
            LOG.error("Total threads: {}", threadBean.getThreadCount());
            LOG.error("Peak thread count: {}", threadBean.getPeakThreadCount());
            LOG.error("Total memory: {} MB", runtime.totalMemory() / (1024 * 1024));
            LOG.error("Free memory: {} MB", runtime.freeMemory() / (1024 * 1024));
            LOG.error("Max memory: {} MB", runtime.maxMemory() / (1024 * 1024));

            // Log thread dump if it's a termination signal
            if ("TERM".equals(Thread.currentThread().getName())
                    || "INT".equals(Thread.currentThread().getName())) {
                ThreadUtils.errorLogThreadDump(LOG);
            }
        } catch (Exception e) {
            LOG.error("Failed to log process state", e);
        }
    }

    private void attemptSignalSourceIdentification(sun.misc.Signal signal) {
        LOG.error("=== SIGNAL SOURCE ANALYSIS ===");

        try {
            // Check if it's a system signal (like OOM killer)
            if (isSystemSignal(signal)) {
                LOG.error(
                        "Signal appears to be from system (possibly OOM killer or system shutdown)");
            }

            // Check if it's from a user process
            if (isUserProcessSignal(signal)) {
                LOG.error("Signal appears to be from user process (kill command, etc.)");
            }

            // Check if it's from container orchestration
            if (isContainerSignal(signal)) {
                LOG.error(
                        "Signal appears to be from container orchestration (Docker, Kubernetes, etc.)");
            }

            // Log parent process information
            logParentProcessInfo();

        } catch (Exception e) {
            LOG.error("Failed to analyze signal source", e);
        }
    }

    private boolean isSystemSignal(sun.misc.Signal signal) {
        // SIGTERM and SIGINT are commonly used by system processes
        return "TERM".equals(signal.getName()) || "INT".equals(signal.getName());
    }

    private boolean isUserProcessSignal(sun.misc.Signal signal) {
        // SIGUSR1 and SIGUSR2 are typically user-defined signals
        return "USR1".equals(signal.getName()) || "USR2".equals(signal.getName());
    }

    private boolean isContainerSignal(sun.misc.Signal signal) {
        // Check if running in container environment
        return System.getenv("KUBERNETES_SERVICE_HOST") != null
                || System.getenv("DOCKER_CONTAINER") != null
                || new File("/.dockerenv").exists()
                || new File("/proc/1/cgroup").exists()
                        && readFileContent("/proc/1/cgroup").contains("docker");
    }

    private void logParentProcessInfo() {
        try {
            String parentPid = getParentProcessId();
            if (parentPid != null) {
                LOG.error("Parent process ID: {}", parentPid);

                // Try to get parent process name
                String parentCmdline = readFileContent("/proc/" + parentPid + "/cmdline");
                if (parentCmdline != null && !parentCmdline.trim().isEmpty()) {
                    LOG.error("Parent process command: {}", parentCmdline.replace('\0', ' '));
                }

                // Try to get parent process status
                String parentStatus = readFileContent("/proc/" + parentPid + "/status");
                if (parentStatus != null) {
                    String[] lines = parentStatus.split("\n");
                    for (String line : lines) {
                        if (line.startsWith("Name:") || line.startsWith("State:")) {
                            LOG.error(
                                    "Parent process {}: {}",
                                    line.split(":")[0],
                                    line.split(":")[1].trim());
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to get parent process information", e);
        }
    }

    private String getProcessId() {
        try {
            return ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
        } catch (Exception e) {
            return "unknown";
        }
    }

    private String getParentProcessId() {
        try {
            String status = readFileContent("/proc/self/status");
            if (status != null) {
                for (String line : status.split("\n")) {
                    if (line.startsWith("PPid:")) {
                        return line.split(":")[1].trim();
                    }
                }
            }
        } catch (Exception e) {
            LOG.debug("Failed to get parent process ID", e);
        }
        return null;
    }

    private String readFileContent(String filePath) {
        try {
            Path path = Paths.get(filePath);
            if (Files.exists(path)) {
                return new String(Files.readAllBytes(path));
            }
        } catch (IOException e) {
            LOG.debug("Failed to read file: {}", filePath, e);
        }
        return null;
    }

    private void signalLoggingLoop() {
        while (!shutdownRequested) {
            try {
                Thread.sleep(30000); // Log every 30 seconds
                if (!shutdownRequested) {
                    logPeriodicSignalStatus();
                }
            } catch (InterruptedException e) {
                if (!shutdownRequested) {
                    LOG.warn("Signal logging thread interrupted");
                }
                break;
            } catch (Exception e) {
                LOG.error("Error in signal logging loop", e);
            }
        }
    }

    private void logPeriodicSignalStatus() {
        try {
            Runtime runtime = Runtime.getRuntime();
            long uptime = System.currentTimeMillis() - START_TIME.get();

            LOG.debug("=== PERIODIC SIGNAL STATUS ===");
            LOG.debug("Process uptime: {} ms", uptime);
            LOG.debug(
                    "Memory usage: {} MB / {} MB",
                    (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024),
                    runtime.maxMemory() / (1024 * 1024));
            LOG.debug("Signal handlers active: {}", INITIALIZED.get());
        } catch (Exception e) {
            LOG.error("Error during periodic signal status logging", e);
        }
    }

    /** Log a signal-related warning with enhanced context information. */
    public static void logSignalWarning(String message, Object... args) {
        LOG.warn("=== SIGNAL WARNING ===");
        LOG.warn(message, args);
        LOG.warn("Process uptime: {} ms", System.currentTimeMillis() - START_TIME.get());
        LOG.warn("Process ID: {}", INSTANCE.getProcessId());
    }

    /** Check if signal handling is active. */
    public static boolean isActive() {
        return INITIALIZED.get();
    }
}
