/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.client.impl.consumer;

import com.rocketmq.client.impl.factory.MQClientInstance;
import com.rocketmq.remoting.common.ServiceThread;

/**
 *
 * @author xuleyan
 * @version RebalanceService.java, v 0.1 2020-12-15 9:31 下午
 */
public class RebalanceService extends ServiceThread {
    /**
     * 等待间隔，单位：毫秒
     */
    private static long waitInterval =
            Long.parseLong(System.getProperty(
                    "rocketmq.client.rebalance.waitInterval", "20000"));

    /**
     * MQClient对象
     */
    private final MQClientInstance mqClientFactory;

    public RebalanceService(MQClientInstance mqClientFactory) {
        this.mqClientFactory = mqClientFactory;
    }

    @Override
    public String getServiceName() {
        return RebalanceService.class.getSimpleName();
    }

    @Override
    public void run() {

    }
}