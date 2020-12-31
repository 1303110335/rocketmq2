/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.client.impl.consumer;

import com.rocketmq.client.impl.factory.MQClientInstance;
import com.rocketmq.remoting.common.ServiceThread;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 *
 * @author xuleyan
 * @version PullMessageService.java, v 0.1 2020-12-15 9:30 下午
 */
@Slf4j
public class PullMessageService extends ServiceThread {

    /**
     * MQClient对象
     */
    private final MQClientInstance mQClientFactory;
    /**
     * 定时器。用于延迟提交拉取请求
     */
    private final ScheduledExecutorService scheduledExecutorService = Executors
            .newSingleThreadScheduledExecutor(new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "PullMessageServiceScheduledThread");
                }
            });

    public PullMessageService(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    @Override
    public String getServiceName() {
        return PullMessageService.class.getSimpleName();
    }

    @Override
    public void run() {

    }
}