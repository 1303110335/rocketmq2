/**
 * xuleyan
 * Copyright (C) 2013-2021 All Rights Reserved.
 */
package com.rocketmq.client.latency;

import com.rocketmq.client.impl.producer.TopicPublishInfo;
import com.rocketmq.common.message.MessageQueue;
import lombok.extern.slf4j.Slf4j;

/**
 * @author xuleyan
 * @version MQFaultStrategy.java, v 0.1 2021-01-02 4:58 PM xuleyan
 */
@Slf4j
public class MQFaultStrategy {

    /**
     * 延迟故障容错，维护每个Broker的发送消息的延迟
     * key：brokerName
     */
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();
    /**
     * 发送消息延迟容错开关
     */
    private boolean sendLatencyFaultEnable = false;
    /**
     * 延迟级别数组
     */
    private Long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};

    /**
     * 根据 Topic发布信息 选择一个消息队列
     * 默认情形下向所有Broker的MessageQueue按顺序轮流发送
     *
     * @param tpInfo         Topic发布信息
     * @param lastBrokerName
     * @return 消息队列
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        if (sendLatencyFaultEnable) {
            //如果开启了延迟容错机制，默认未开启
            // @todo 后面再写
        }
        // 默认情况下,获得 lastBrokerName 对应的一个消息队列，不考虑该队列的可用性
        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }
}