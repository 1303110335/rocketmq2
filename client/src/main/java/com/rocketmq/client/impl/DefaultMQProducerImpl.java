/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.client.impl;

import com.rocketmq.client.Validators;
import com.rocketmq.client.exception.MQBrokerException;
import com.rocketmq.client.exception.MQClientException;
import com.rocketmq.client.impl.factory.MQClientInstance;
import com.rocketmq.client.impl.producer.TopicPublishInfo;
import com.rocketmq.client.producer.DefaultMQProducer;
import com.rocketmq.client.producer.SendCallback;
import com.rocketmq.client.producer.SendResult;
import com.rocketmq.client.producer.TransactionCheckListener;
import com.rocketmq.common.MixAll;
import com.rocketmq.common.ServiceState;
import com.rocketmq.common.help.FAQUrl;
import com.rocketmq.common.message.Message;
import com.rocketmq.common.message.MessageExt;
import com.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import com.rocketmq.remoting.RPCHook;
import com.rocketmq.remoting.exception.RemotingException;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author xuleyan
 * @version DefaultMQProducerImpl.java, v 0.1 2020-12-14 9:48 下午
 */
@Slf4j
public class DefaultMQProducerImpl implements MQProducerInner {
    private final DefaultMQProducer defaultMQProducer;
    private final RPCHook rpcHook;
    private ServiceState serviceState = ServiceState.CREATE_JUST;
    private final ConcurrentHashMap<String/* topic */, TopicPublishInfo> topicPublishInfoTable = new ConcurrentHashMap<String, TopicPublishInfo>();

    private MQClientInstance mQClientFactory;

    public DefaultMQProducerImpl(final DefaultMQProducer defaultMQProducer) {
        this(defaultMQProducer, null);
    }

    public DefaultMQProducerImpl(final DefaultMQProducer defaultMQProducer, RPCHook rpcHook) {
        this.defaultMQProducer = defaultMQProducer;
        this.rpcHook = rpcHook;
    }

    public void start() throws MQClientException {
        this.start(true);
    }

    public void start(final boolean startFactory) throws MQClientException {
        switch (this.serviceState) {
            case CREATE_JUST:
                this.serviceState = ServiceState.START_FAILED;
                this.checkConfig();

                if (!this.defaultMQProducer.getProducerGroup().equals(MixAll.CLIENT_INNER_PRODUCER_GROUP)) {
                    this.defaultMQProducer.changeInstanceNameToPID();
                }
                // 获取MQClient 对象
                this.mQClientFactory = MQClientManager.getInstance().getAndCreateMQClientInstance(this.defaultMQProducer, rpcHook);

                // 注册Producer
                boolean registerOK = mQClientFactory.registerProducer(this.defaultMQProducer.getProducerGroup(), this);
                if (!registerOK) {
                    this.serviceState = ServiceState.CREATE_JUST;
                    throw new MQClientException("The producer group[" + this.defaultMQProducer.getProducerGroup()
                            + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL),
                            null);
                }

                //把 MixAll.DEFAULT_TOPIC 放入其中
                this.topicPublishInfoTable.put(this.defaultMQProducer.getCreateTopicKey(), new TopicPublishInfo());

                if (startFactory) {
                    mQClientFactory.start();
                }

                log.info("the producer [{}] start OK. sendMessageWithVIPChannel={}", this.defaultMQProducer.getProducerGroup(),
                        this.defaultMQProducer.isSendMessageWithVIPChannel());
                // 标记初始化成功
                this.serviceState = ServiceState.RUNNING;
                break;
            case RUNNING:
            case START_FAILED:
            case SHUTDOWN_ALREADY:
                throw new MQClientException("The producer service state not OK, maybe started once, "//
                        + this.serviceState//
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                        null);
            default:
                break;
        }
        this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
    }

    /**
     * DEFAULT SYNC -------------------------------------------------------
     */
    public SendResult send(Message msg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return send(msg, this.defaultMQProducer.getSendMsgTimeout());
    }

    public SendResult send(Message msg, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.sendDefaultImpl(msg, CommunicationMode.SYNC, null, timeout);
    }

    /**
     * 发送消息。
     * 1. 获取消息路由信息
     * 2. 选择要发送到的消息队列
     * 3. 执行消息发送核心方法
     * 4. 对发送结果进行封装返回
     *
     * @param msg               消息
     * @param communicationMode 通信模式
     * @param sendCallback      发送回调
     * @param timeout           发送消息请求超时时间
     * @return 发送结果
     * @throws MQClientException    当Client发生异常
     * @throws RemotingException    当请求发生异常
     * @throws MQBrokerException    当Broker发生异常
     * @throws InterruptedException 当线程被打断
     */
    private SendResult sendDefaultImpl(
            Message msg,
            final CommunicationMode communicationMode,
            final SendCallback sendCallback,
            final long timeout) {
        return null;
    }

    /**
     * 校验producer配置
     * 0. group不能为空
     * 1. group格式是否正确，A-Za-z0-9
     * 2. group不能等于MixAll.DEFAULT_PRODUCER_GROUP
     *
     * @throws MQClientException 校验异常
     */
    private void checkConfig() throws MQClientException {
        Validators.checkGroup(this.defaultMQProducer.getProducerGroup());

        if (null == this.defaultMQProducer.getProducerGroup()) {
            throw new MQClientException("producerGroup is null", null);
        }

        if (this.defaultMQProducer.getProducerGroup().equals(MixAll.DEFAULT_PRODUCER_GROUP)) {
            throw new MQClientException("producerGroup can not equal " + MixAll.DEFAULT_PRODUCER_GROUP + ", please specify another one.",
                    null);
        }
    }

    @Override
    public Set<String> getPublishTopicList() {
        Set<String> topicList = new HashSet<String>();
        for (String key : this.topicPublishInfoTable.keySet()) {
            topicList.add(key);
        }

        return topicList;
    }

    @Override
    public boolean isPublishTopicNeedUpdate(String topic) {
        TopicPublishInfo prev = this.topicPublishInfoTable.get(topic);
        return null == prev || !prev.ok();
    }

    @Override
    public TransactionCheckListener checkListener() {
        return null;
    }

    @Override
    public void checkTransactionState(String addr, MessageExt msg, CheckTransactionStateRequestHeader checkRequestHeader) {

    }

    /**
     * 更新 Topic 路由信息
     *
     * @param topic Topic
     * @param info  Topic 路由信息
     */
    @Override
    public void updateTopicPublishInfo(final String topic, final TopicPublishInfo info) {
        if (topic != null && info != null) {
            TopicPublishInfo prev = this.topicPublishInfoTable.put(topic, info);
            if (prev != null) {
                log.info("updateTopicPublishInfo prev is not null, " + prev.toString());
            }
        }
    }

    @Override
    public boolean isUnitMode() {
        return false;
    }


}