/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.client.impl.producer;

import com.rocketmq.client.impl.factory.MQClientInstance;

/**
 *
 * @author xuleyan
 * @version MQAdminImpl.java, v 0.1 2020-12-15 9:29 下午
 */
public class MQAdminImpl {

    private final MQClientInstance mQClientFactory;
    private long timeoutMillis = 6000;

    public MQAdminImpl(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }
}