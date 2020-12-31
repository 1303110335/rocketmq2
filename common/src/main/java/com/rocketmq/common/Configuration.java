/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.common;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author xuleyan
 * @version Configuration.java, v 0.1 2020-10-31 5:01 下午
 */
@Slf4j
public class Configuration {

    private List<Object> configObjectList = new ArrayList<>(4);
    private String storePath;
    private boolean storePathFromConfig = false;
    private Object storePathObject;
    private Field storePathField;
    private DataVersion dataVersion = new DataVersion();
    private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    private Properties allConfigs = new Properties();

    public Configuration(Object... configObjects) {
        if (configObjects == null || configObjects.length == 0) {
            return;
        }
        for (Object configObject : configObjects) {
            registerConfig(configObject);
        }
    }

    /**
     * register com.rocketmq.store.config object
     *
     * @param configObject
     */
    public Configuration registerConfig(Object configObject) {
        try {
            readWriteLock.writeLock().lockInterruptibly();
            try {
                Properties registerProps = MixAll.object2Properties(configObject);
                merge(registerProps, this.allConfigs);
                configObjectList.add(configObject);
            } finally {
                readWriteLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("registerConfig lock error");
        }
        return this;
    }

    private void merge(Properties from, Properties to) {
        for (Object key : from.keySet()) {
            Object fromObj = from.get(key), toObj = to.get(key);
            if (toObj != null && !toObj.equals(fromObj)) {
                log.info("Replace, key: {}, value: {} -> {}", key, toObj, fromObj);
            }
            to.put(key, fromObj);
        }
    }

    public void persist() {
        try {
            readWriteLock.readLock().lockInterruptibly();

            try {
                String allConfigs = getAllConfigsInternal();
                MixAll.string2File(allConfigs, getStorePath());
            } catch (IOException e) {
                log.error("persist string2File error, ", e);
            } finally {
                readWriteLock.readLock().unlock();
            }

        } catch (InterruptedException e) {
            log.error("persist lock error");
        }
    }

    private String getStorePath() {
        String realStorePath = null;
        try {
            readWriteLock.readLock().lockInterruptibly();

            try {
                realStorePath = this.storePath;

                if (this.storePathFromConfig) {
                    try {
                        realStorePath = (String) storePathField.get(this.storePathObject);
                    } catch (IllegalAccessException e) {
                        log.error("getStorePath error, ", e);
                    }
                }
            } finally {
                readWriteLock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    private String getAllConfigsInternal() {
        StringBuilder stringBuilder = new StringBuilder();
        for (Object configObject : this.configObjectList) {
            Properties properties = MixAll.object2Properties(configObject);
            if (properties != null) {
                merge(properties, this.allConfigs);
            } else {
                log.warn("getAllConfigsInternal object2Properties is null, {}", configObject.getClass());
            }
        }

        {
            stringBuilder.append(MixAll.properties2String(this.allConfigs));
        }
        return stringBuilder.toString();
    }
}