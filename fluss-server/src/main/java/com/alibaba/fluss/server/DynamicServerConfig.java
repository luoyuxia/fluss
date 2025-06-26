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

package com.alibaba.fluss.server;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.config.ConfigOption;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.config.dynamic.ServerReconfigurable;
import com.alibaba.fluss.exception.ConfigException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.alibaba.fluss.utils.concurrent.LockUtils.inWriteLock;

/**
 * DynamicServerConfig, 使用时如下： 如果一个ServerReconfigurable实现类想要监听配置变化，并且通过getRegistry注册进来. 后续{@link
 * DynamicConfigManager} 监听变更后，会调用getConfigHandler更新配置项，并且推送给这些ServerReconfigurable. TODO：
 * 添加test:类似ConfigurationTest.
 */
@Internal
public class DynamicServerConfig extends Configuration {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicServerConfig.class);
    private final Set<ServerReconfigurable> serverReconfigurableSet = ConcurrentHashMap.newKeySet();

    /** The initial configuration items when the server starts from server.yaml. */
    private final Map<String, String> initialConfig;

    /** The dynamic configuration items that are added during running(stored in zk). */
    private final Map<String, String> dynamicConfigs = new HashMap<>();

    /**
     * The current configuration, which is a combination of initial configuration and dynamic
     * configuration.
     */
    private volatile Configuration currentConfig;

    private final Map<String, String> currentConfigMap;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Set<String> allowedConfigKeys;
    private final Set<String> allowedConfigPrefixes;

    public DynamicServerConfig(Configuration flussConfig) {
        this.currentConfig = flussConfig;
        this.initialConfig = flussConfig.toMap();
        this.currentConfigMap = flussConfig.toMap();
        this.allowedConfigKeys = new HashSet<>();
        this.allowedConfigPrefixes = new HashSet<>();
    }

    /** Register a ServerReconfigurable which listens to configuration changes. */
    public void register(ServerReconfigurable serverReconfigurable) {
        ServerReconfigurable.AllowedConfigs allowedConfigs = serverReconfigurable.allowedConfigs();
        serverReconfigurableSet.add(serverReconfigurable);
        allowedConfigKeys.addAll(allowedConfigs.getExactConfigKeys());
        allowedConfigPrefixes.addAll(allowedConfigs.getConfigKeyPrefixes());
    }

    /** Update the dynamic configuration and apply to registered ServerReconfigurables. */
    public void updateDynamicConfig(Map<String, String> newDynamicConfigs) throws ConfigException {
        newDynamicConfigs.forEach(
                (key, value) -> {
                    if (!isAllowedConfig(key)) {
                        throw new ConfigException(
                                String.format(
                                        "The config key %s is not allowed to be changed dynamically.",
                                        key));
                    }
                });

        inWriteLock(lock, () -> updateCurrentConfig(newDynamicConfigs));
    }

    public Map<String, String> getDynamicConfigs() {
        return dynamicConfigs;
    }

    public Map<String, String> getInitialServerConfigs() {
        return initialConfig;
    }

    // --- Override Configuration methods, 看是否要改为反射, Todo: 详细测试获取所有类型 ---
    @Override
    public <T> Optional<T> getOptional(ConfigOption<T> option) {
        return currentConfig.getOptional(option);
    }

    @Override
    public String getValue(ConfigOption<?> configOption) {
        return currentConfig.getValue(configOption);
    }

    @Override
    public <T extends Enum<T>> T getEnum(Class<T> enumClass, ConfigOption<String> configOption) {
        return currentConfig.getEnum(enumClass, configOption);
    }

    @Override
    public byte[] getBytes(String key, byte[] defaultValue) {
        return currentConfig.getBytes(key, defaultValue);
    }

    @Override
    public Set<String> keySet() {
        return currentConfig.keySet();
    }

    @Override
    public boolean containsKey(String key) {
        return currentConfig.containsKey(key);
    }

    @Override
    public boolean contains(ConfigOption<?> configOption) {
        return currentConfig.contains(configOption);
    }

    @Override
    public Map<String, String> toMap() {
        return currentConfig.toMap();
    }

    @Override
    public <T> Class<T> getClass(
            String key, Class<? extends T> defaultValue, ClassLoader classLoader)
            throws ClassNotFoundException {
        return currentConfig.getClass(key, defaultValue, classLoader);
    }

    private boolean isAllowedConfig(String key) {
        if (allowedConfigKeys.contains(key)) {
            return true;
        }

        for (String prefix : allowedConfigPrefixes) {
            if (key.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    private void updateCurrentConfig(Map<String, String> newDynamicConfigs) throws ConfigException {
        Map<String, String> newProps = new HashMap<>(initialConfig);
        overrideProps(newProps, newDynamicConfigs);
        Configuration newConfig = Configuration.fromMap(newProps);
        Configuration oldConfig = currentConfig;
        Set<ServerReconfigurable> appliedServerReconfigurableSet = new HashSet<>();
        if (!newProps.equals(currentConfigMap)) {
            serverReconfigurableSet.forEach(
                    serverReconfigurable -> serverReconfigurable.validate(newConfig));
            try {
                for (ServerReconfigurable serverReconfigurable : serverReconfigurableSet) {
                    serverReconfigurable.reconfigure(newConfig);
                    appliedServerReconfigurableSet.add(serverReconfigurable);
                }
            } catch (Exception e) {
                LOG.error("Apply new dynamic error and will roll back all the applied config.", e);
                if (e instanceof ConfigException) {
                    // todo: add more
                    throw new ConfigException(e.getMessage());
                }
                appliedServerReconfigurableSet.forEach(
                        serverReconfigurable -> serverReconfigurable.reconfigure(oldConfig));
                throw e;
            }

            currentConfig = newConfig;
            currentConfigMap.clear();
            dynamicConfigs.clear();
            currentConfigMap.putAll(newProps);
            dynamicConfigs.putAll(newDynamicConfigs);
            LOG.info("Dynamic configs changed: {}", newDynamicConfigs);
        }
    }

    private void overrideProps(Map<String, String> props, Map<String, String> propsOverride) {
        // todo: 后续增加大小写敏感
        propsOverride.forEach(
                (key, value) -> {
                    if (value == null) {
                        props.remove(key);
                    } else {
                        props.put(key, value);
                    }
                });
    }
}
