/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.broker;


import com.rocketmq.broker.client.ClientHousekeepingService;
import com.rocketmq.broker.client.ProducerManager;
import com.rocketmq.broker.out.BrokerOuterAPI;
import com.rocketmq.broker.processor.ClientManageProcessor;
import com.rocketmq.broker.topic.TopicConfigManager;
import com.rocketmq.common.BrokerConfig;
import com.rocketmq.common.Configuration;
import com.rocketmq.common.ThreadFactoryImpl;
import com.rocketmq.common.annotation.ImportantField;
import com.rocketmq.common.constant.PermName;
import com.rocketmq.common.namesrv.RegisterBrokerResult;
import com.rocketmq.common.protocol.RequestCode;
import com.rocketmq.common.protocol.TopicConfig;
import com.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import com.rocketmq.remoting.RemotingServer;
import com.rocketmq.remoting.common.RemotingUtil;
import com.rocketmq.remoting.exception.RemotingCommandException;
import com.rocketmq.remoting.exception.RemotingConnectException;
import com.rocketmq.remoting.exception.RemotingSendRequestException;
import com.rocketmq.remoting.exception.RemotingTimeoutException;
import com.rocketmq.remoting.netty.NettyClientConfig;
import com.rocketmq.remoting.netty.NettyRemotingServer;
import com.rocketmq.remoting.netty.NettyServerConfig;
import com.rocketmq.store.config.MessageStoreConfig;
import com.rocketmq.client.exception.MQBrokerException;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.concurrent.*;

/**
 * @author xuleyan
 * @version BrokerController.java, v 0.1 2020-10-29 9:15 下午
 */
@Slf4j
public class BrokerController {

    private final BrokerConfig brokerConfig;
    private final NettyServerConfig nettyServerConfig;
    private final NettyClientConfig nettyClientConfig;

    private Configuration configuration;
    private final BrokerOuterAPI brokerOuterAPI;

    private RemotingServer remotingServer;
    private final ProducerManager producerManager;

    private TopicConfigManager topicConfigManager;
    private final MessageStoreConfig messageStoreConfig;
    private final ClientHousekeepingService clientHousekeepingService;
    private ExecutorService clientManageExecutor;
    private final BlockingQueue<Runnable> clientManagerThreadPoolQueue;

    @ImportantField
    private String brokerIP1 = RemotingUtil.getLocalAddress();
    private String brokerIP2 = RemotingUtil.getLocalAddress();

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
            "BrokerControllerScheduledThread"));


    public BrokerController(BrokerConfig brokerConfig, NettyServerConfig nettyServerConfig, NettyClientConfig nettyClientConfig, MessageStoreConfig messageStoreConfig) {
        this.brokerConfig = brokerConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.nettyClientConfig = nettyClientConfig;
        this.brokerOuterAPI = new BrokerOuterAPI(nettyClientConfig);
        this.messageStoreConfig = messageStoreConfig;
        this.configuration = new Configuration();
        this.topicConfigManager = new TopicConfigManager(this);

        this.clientHousekeepingService = new ClientHousekeepingService(this);
        this.producerManager = new ProducerManager();
        this.clientManagerThreadPoolQueue = new LinkedBlockingDeque<>(this.brokerConfig.getClientManagerThreadPoolQueueCapacity());
    }

    public boolean initialize() {
        boolean result = true;
        //加载 topics.json
        result = result && this.topicConfigManager.load();

        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, clientHousekeepingService);

        if (this.brokerConfig.getNamesrvAddr() != null) {
            this.brokerOuterAPI.updateNameServerAddressList(this.brokerConfig.getNamesrvAddr());
        }

        this.clientManageExecutor = new ThreadPoolExecutor(
                this.brokerConfig.getClientManageThreadPoolNums(),
                this.brokerConfig.getClientManageThreadPoolNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.clientManagerThreadPoolQueue,
                new ThreadFactoryImpl("ClientManageThread_")
        );

        if (result) {
            this.registerProcessor();
        }
        return true;
    }

    private void registerProcessor() {
        /**
         * ClientManageProcessor
         */
        ClientManageProcessor clientProcessor = new ClientManageProcessor(this);
        this.remotingServer.registerProcessor(RequestCode.HEART_BEAT, clientProcessor, this.clientManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.UNREGISTER_CLIENT, clientProcessor, this.clientManageExecutor);


    }

    public TopicConfigManager getTopicConfigManager() {
        return topicConfigManager;
    }

    public void setTopicConfigManager(TopicConfigManager topicConfigManager) {
        this.topicConfigManager = topicConfigManager;
    }

    public String getBrokerIP1() {
        return brokerIP1;
    }

    public void setBrokerIP1(String brokerIP1) {
        this.brokerIP1 = brokerIP1;
    }

    public String getBrokerIP2() {
        return brokerIP2;
    }

    public void setBrokerIP2(String brokerIP2) {
        this.brokerIP2 = brokerIP2;
    }

    public String getHAServerAddr() {
        return this.brokerConfig.getBrokerIP2() + ":" + this.messageStoreConfig.getHaListenPort();
    }

    public ProducerManager getProducerManager() {
        return producerManager;
    }

    public void shutdown() {
        this.remotingServer.shutdown();
    }

    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public NettyClientConfig getNettyClientConfig() {
        return nettyClientConfig;
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public String getBrokerAddr() {
        return this.brokerConfig.getBrokerIP1() + ":" + this.nettyServerConfig.getListenPort();
    }

    /**
     * 向Namesrc注册Broker信息,每隔30S执行一次
     *
     * @param oneway
     */
    public synchronized void registerBrokerAll(boolean oneway) throws InterruptedException, RemotingTimeoutException,
            RemotingSendRequestException, RemotingConnectException, RemotingCommandException, MQBrokerException {
        TopicConfigSerializeWrapper topicConfigWrapper = this.getTopicConfigManager().buildTopicConfigSerializeWrapper();

        if (!PermName.isWriteable(this.getBrokerConfig().getBrokerPermission())
                || !PermName.isReadable(this.getBrokerConfig().getBrokerPermission())) {
            ConcurrentHashMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<String, TopicConfig>();
            for (TopicConfig topicConfig : topicConfigWrapper.getTopicConfigTable().values()) {
                TopicConfig tmp =
                        new TopicConfig(topicConfig.getTopicName(), topicConfig.getReadQueueNums(), topicConfig.getWriteQueueNums(),
                                this.brokerConfig.getBrokerPermission());
                topicConfigTable.put(topicConfig.getTopicName(), tmp);
            }
            topicConfigWrapper.setTopicConfigTable(topicConfigTable);
        }
        RegisterBrokerResult registerBrokerResult = this.brokerOuterAPI.registerBrokerAll(
                this.brokerConfig.getBrokerClusterName(),
                this.getBrokerAddr(),
                this.brokerConfig.getBrokerName(),
                this.brokerConfig.getBrokerId(),
                this.getHAServerAddr(),
                topicConfigWrapper,
                new ArrayList<>(),
                oneway,
                this.brokerConfig.getRegisterBrokerTimeoutMills());
    }

    public void start() throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, RemotingCommandException, MQBrokerException {
        // 一堆初始化...

        if (this.remotingServer != null) {
            this.remotingServer.start();
        }

        if (this.brokerOuterAPI != null) {
            this.brokerOuterAPI.start();
        }

        this.registerBrokerAll(false);

        // 10s之后每隔30s进行注册
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    BrokerController.this.registerBrokerAll(false);
                } catch (Throwable e) {
                    log.error("registerBrokerAll Exception", e);
                }
            }
        }, 1000 * 10, 1000 * 30, TimeUnit.MILLISECONDS);

    }
}