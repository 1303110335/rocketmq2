/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.client.impl;

import com.rocketmq.client.Validators;
import com.rocketmq.client.exception.MQBrokerException;
import com.rocketmq.client.exception.MQClientException;
import com.rocketmq.client.hook.SendMessageContext;
import com.rocketmq.client.impl.factory.MQClientInstance;
import com.rocketmq.client.impl.producer.TopicPublishInfo;
import com.rocketmq.client.latency.MQFaultStrategy;
import com.rocketmq.client.producer.DefaultMQProducer;
import com.rocketmq.client.producer.SendCallback;
import com.rocketmq.client.producer.SendResult;
import com.rocketmq.client.producer.TransactionCheckListener;
import com.rocketmq.common.MixAll;
import com.rocketmq.common.ServiceState;
import com.rocketmq.common.help.FAQUrl;
import com.rocketmq.common.message.Message;
import com.rocketmq.common.message.MessageClientIDSetter;
import com.rocketmq.common.message.MessageConst;
import com.rocketmq.common.message.MessageDecoder;
import com.rocketmq.common.message.MessageExt;
import com.rocketmq.common.message.MessageQueue;
import com.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import com.rocketmq.common.protocol.header.SendMessageRequestHeader;
import com.rocketmq.remoting.RPCHook;
import com.rocketmq.remoting.exception.RemotingException;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Random;
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
    private final Random random = new Random();
    private ServiceState serviceState = ServiceState.CREATE_JUST;
    private final ConcurrentHashMap<String/* topic */, TopicPublishInfo> topicPublishInfoTable = new ConcurrentHashMap<String, TopicPublishInfo>();

    private MQClientInstance mQClientFactory;

    private MQFaultStrategy mqFaultStrategy = new MQFaultStrategy();

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
            final long timeout) throws MQClientException {

        this.makeSureStateOK();
        // 校验消息格式
        Validators.checkMessage(msg, this.defaultMQProducer);
        // 调用编号；用于下面打印日志，标记为同一次发送消息
        final long invokeID = random.nextLong();
        long beginTimestampFirst = System.currentTimeMillis();
        long beginTimestampPrev = beginTimestampFirst;
        @SuppressWarnings("UnusedAssignment")
        long endTimestamp = beginTimestampFirst;

        TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(msg.getTopic());
        if (topicPublishInfo != null && topicPublishInfo.ok()) {
            MessageQueue mq = null;
            SendResult sendResult = null;
            // 同步3次，异步1次
            int timesTotal = communicationMode == CommunicationMode.SYNC ? 1 + this.defaultMQProducer.getRetryTimesWhenSendFailed() : 1;
            // 第几次发送
            int times = 0;
            String[] brokersSent = new String[timesTotal];
            for (; times < timesTotal; times ++) {
                String lastBrokerName = null == mq ? null : mq.getBrokerName();
                MessageQueue tmpmq = this.selectOneMessageQueue(topicPublishInfo, lastBrokerName);
                if (tmpmq != null) {
                    mq = tmpmq;
                    brokersSent[times] = mq.getBrokerName();
                    try {
                        beginTimestampPrev = System.currentTimeMillis();
                        // 调用发送消息核心方法
                        sendResult = this.sendKernelImpl(msg, mq, communicationMode, sendCallback, topicPublishInfo, timeout);

                    } catch (Exception ex) {

                    }
                } else {
                    break;
                }

            }
        }
        return null;
    }

    /**
     * 发送消息核心方法，  并返回发送结果
     *
     * @param msg               消息
     * @param mq                消息队列
     * @param communicationMode 通信模式
     * @param sendCallback      发送回调
     * @param topicPublishInfo  Topic发布信息
     * @param timeout           发送消息请求超时时间
     * @return 发送结果
     * @throws MQClientException 当Client发生异常
     * @throws RemotingException 当请求发生异常
     * @throws MQBrokerException 当Broker发生异常
     * @throws InterruptedException 当线程被打断
     */
    private SendResult sendKernelImpl(final Message msg,
                                      final MessageQueue mq,
                                      final CommunicationMode communicationMode,
                                      final SendCallback sendCallback,
                                      final TopicPublishInfo topicPublishInfo,
                                      final long timeout) {
        String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        if (null == brokerAddr) {
            tryToFindTopicPublishInfo(mq.getTopic());
            brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        }
        SendMessageContext context = null;
        if (brokerAddr != null) {
            // 记录消息内容。下面逻辑可能改变消息内容，例如消息压缩。
            byte[] prevBody = msg.getBody();
            try {
                // 设置uniqID,填充入Properties
                MessageClientIDSetter.setUniqID(msg);
                // 消息压缩
                int sysFlag = 0;
                // 事务
                final String tranMsg = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);

                SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
                requestHeader.setProducerGroup(this.defaultMQProducer.getProducerGroup());
                requestHeader.setTopic(msg.getTopic());
                requestHeader.setDefaultTopic(this.defaultMQProducer.getCreateTopicKey());
                requestHeader.setDefaultTopicQueueNums(this.defaultMQProducer.getDefaultTopicQueueNums());
                requestHeader.setQueueId(mq.getQueueId());
                requestHeader.setSysFlag(sysFlag);
                requestHeader.setBornTimestamp(System.currentTimeMillis());
                requestHeader.setFlag(msg.getFlag());
                requestHeader.setProperties(MessageDecoder.messageProperties2String(msg.getProperties()));
                requestHeader.setReconsumeTimes(0);
                requestHeader.setUnitMode(this.isUnitMode());

                // 发送消息
                SendResult sendResult = null;
                switch (communicationMode) {
                    case ASYNC:
                        sendResult = this.mQClientFactory.getMQClientAPIImpl().sendMessage(//
                                brokerAddr, // 1
                                mq.getBrokerName(), // 2
                                msg, // 3
                                requestHeader, // 4
                                timeout, // 5
                                communicationMode, // 6
                                sendCallback, // 7
                                topicPublishInfo, // 8
                                this.mQClientFactory, // 9
                                this.defaultMQProducer.getRetryTimesWhenSendAsyncFailed(), // 10
                                context, //
                                this);
                        break;
                    case ONEWAY:
                    case SYNC:
                        sendResult = this.mQClientFactory.getMQClientAPIImpl().sendMessage(
                                brokerAddr,
                                mq.getBrokerName(),
                                msg,
                                requestHeader,
                                timeout,
                                communicationMode,
                                context,
                                this);
                        break;
                    default:
                        break;
                }
            } catch (Exception ex) {

            }

        }
        return null;
    }

    private MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        return this.mqFaultStrategy.selectOneMessageQueue(tpInfo, lastBrokerName);
    }

    /**
     * 获取 Topic发布信息
     * 如果获取不到，或者状态不正确，则从 Namesrv获取一次
     *
     * @param topic Topic
     * @return topic 信息
     */
    private TopicPublishInfo tryToFindTopicPublishInfo(final String topic) {
        // 缓存中获取 Topic发布信息
        TopicPublishInfo topicPublishInfo = this.topicPublishInfoTable.get(topic);
        // 当无可用的 Topic发布信息时，从Namesrv获取一次
        if (null == topicPublishInfo || !topicPublishInfo.ok()) {
            this.topicPublishInfoTable.putIfAbsent(topic, new TopicPublishInfo());
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            topicPublishInfo = this.topicPublishInfoTable.get(topic);
        }

        if (topicPublishInfo.isHaveTopicRouterInfo() || topicPublishInfo.ok()) {
            return topicPublishInfo;
        } else {
            // 使用 {@link DefaultMQProducer#createTopicKey} 对应的 Topic发布信息。用于 Topic发布信息不存在 && Broker支持自动创建Topic
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic, true, this.defaultMQProducer);
            topicPublishInfo = this.topicPublishInfoTable.get(topic);
            return topicPublishInfo;
        }
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

    public void shutdown() {
        this.shutdown(true);
    }

    /**
     * @todo 后面再加
     * @param shutdownFactory
     */
    public void shutdown(final boolean shutdownFactory) {
//        switch (this.serviceState) {
//            case CREATE_JUST:
//                break;
//            case RUNNING:
//                this.mQClientFactory.unregisterProducer(this.defaultMQProducer.getProducerGroup());
//                if (shutdownFactory) {
//                    this.mQClientFactory.shutdown();
//                }
//
//                log.info("the producer [{}] shutdown OK", this.defaultMQProducer.getProducerGroup());
//                this.serviceState = ServiceState.SHUTDOWN_ALREADY;
//                break;
//            case SHUTDOWN_ALREADY:
//                break;
//            default:
//                break;
//        }

        return;
    }

    @Override
    public boolean isUnitMode() {
        return false;
    }


    /**
     * 校验Producer是否处于运行{@link ServiceState#RUNNING}状态。
     *
     * @throws MQClientException 当不处于运行状态抛出client异常
     */
    private void makeSureStateOK() throws MQClientException {
        if (this.serviceState != ServiceState.RUNNING) {
            throw new MQClientException("The producer service state not OK, "//
                    + this.serviceState//
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                    null);
        }
    }
}