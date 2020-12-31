/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.common.namesrv;

import com.rocketmq.common.MixAll;
import lombok.extern.slf4j.Slf4j;

/**
 * @author xuleyan
 * @version NamesrvConfig.java, v 0.1 2020-10-19 10:58 下午
 */
@Slf4j
public class NamesrvConfig {

    private String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));

    public void setRocketMqHome(String dir) {
        this.rocketmqHome = dir;
    }
}