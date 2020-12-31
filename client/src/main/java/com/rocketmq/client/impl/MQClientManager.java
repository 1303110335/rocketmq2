/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.client.impl;

import com.rocketmq.client.ClientConfig;
import com.rocketmq.client.impl.factory.MQClientInstance;
import com.rocketmq.client.producer.DefaultMQProducer;
import com.rocketmq.remoting.RPCHook;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author xuleyan
 * @version MQClientManager.java, v 0.1 2020-12-15 11:18 上午
 */
@Slf4j
public class MQClientManager {
    private static MQClientManager instance = new MQClientManager();
    private ConcurrentHashMap<String/* clientId */, MQClientInstance> factoryTable = new ConcurrentHashMap<>();
    private AtomicInteger factoryIndexGenerator = new AtomicInteger();
    private MQClientManager() {

    }


    public static MQClientManager getInstance() {
        return instance;
    }

    /**
     * 获得 MQClient 对象。如果不存在，则进行创建。
     *
     * @param clientConfig client配置
     * @param rpcHook      rpcHook
     * @return MQ Client Instance
     */
    public MQClientInstance getAndCreateMQClientInstance(ClientConfig clientConfig, RPCHook rpcHook) {
        // 192.168.0.1@10072 ,代表了当前客户端的唯一编号
        String clientId = clientConfig.buildMQClientId();
        MQClientInstance instance = factoryTable.get(clientId);
        if (instance == null) {
            instance = new MQClientInstance(clientConfig.cloneClientConfig(), this.factoryIndexGenerator.getAndIncrement(), clientId, rpcHook);
            MQClientInstance pre = this.factoryTable.putIfAbsent(clientId, instance);
            if (pre == null) {
                log.info("Created new MQClientInstance for clientId:[{}]", clientId);
            } else {
                instance = pre;
                log.warn("Returned Previous MQClientInstance for clientId:[{}]", clientId);
            }
        }

        return instance;
    }
}