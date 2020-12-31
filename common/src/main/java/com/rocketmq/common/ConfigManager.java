/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.common;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 * @author xuleyan
 * @version ConfigManager.java, v 0.1 2020-10-29 10:46 下午
 */
@Slf4j
public abstract class ConfigManager {

    /**
     * 编码内容
     *
     * @return
     */
    public abstract String encode();

    /**
     * 解码内容
     *
     * @param jsonString 内容
     */
    public abstract void decode(final String jsonString);

    /**
     * 加载文件
     *
     * @return
     */
    public boolean load() {
        String fileName = null;

        try {
            fileName = this.configFilePath();
            String jsonString = MixAll.file2String(fileName);
            if (jsonString == null || jsonString.length() == 0) {
                return false;
            }
            this.decode(jsonString);
            log.info("load {} ok", fileName);
            return true;
        } catch (Exception e) {
            log.error("load " + fileName + " Failed, and try to load backup file", e);
            return false;
        }
    }

    /**
     * 配置文件地址
     *
     * @return 配置文件地址
     */
    public abstract String configFilePath();

    /**
     * 持久化
     */
    public synchronized void persist() {
        String jsonString = this.encode(true);
        if (jsonString != null) {
            String fileName = this.configFilePath();
            try {
                MixAll.string2File(jsonString, fileName);
            } catch (IOException e) {
                log.error("persist file Exception, " + fileName, e);
            }
        }
    }

    /**
     * 编码存储内容
     *
     * @param prettyFormat 是否格式化
     * @return 内容
     */
    public abstract String encode(final boolean prettyFormat);
}