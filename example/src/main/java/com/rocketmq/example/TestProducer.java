/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.example;

import com.rocketmq.client.exception.MQClientException;
import com.rocketmq.client.producer.DefaultMQProducer;
import com.rocketmq.common.MixAll;

/**
 *
 * @author xuleyan
 * @version TestProducer.java, v 0.1 2020-12-14 9:33 下午
 */
public class TestProducer {

    public static void main(String[] args) throws MQClientException {
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "127.0.0.1:9876");
        DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName");

        producer.start();


    }
}