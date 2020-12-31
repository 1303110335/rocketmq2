/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.client.impl.factory;

import com.rocketmq.client.ClientConfig;
import com.rocketmq.client.exception.MQClientException;
import com.rocketmq.client.impl.ClientRemotingProcessor;
import com.rocketmq.client.impl.DefaultMQProducerImpl;
import com.rocketmq.client.impl.MQClientAPIImpl;
import com.rocketmq.client.impl.MQProducerInner;
import com.rocketmq.client.impl.consumer.MQConsumerInner;
import com.rocketmq.client.impl.consumer.PullMessageService;
import com.rocketmq.client.impl.consumer.RebalanceService;
import com.rocketmq.client.impl.producer.MQAdminImpl;
import com.rocketmq.client.impl.producer.TopicPublishInfo;
import com.rocketmq.client.producer.DefaultMQProducer;
import com.rocketmq.client.stat.ConsumerStatsManager;
import com.rocketmq.common.MQVersion;
import com.rocketmq.common.MixAll;
import com.rocketmq.common.ServiceState;
import com.rocketmq.common.constant.PermName;
import com.rocketmq.common.message.MessageQueue;
import com.rocketmq.common.protocol.heartbeat.HeartbeatData;
import com.rocketmq.common.protocol.heartbeat.ProducerData;
import com.rocketmq.common.protocol.route.BrokerData;
import com.rocketmq.common.protocol.route.QueueData;
import com.rocketmq.common.protocol.route.TopicRouteData;
import com.rocketmq.remoting.RPCHook;
import com.rocketmq.remoting.RemotingCommand;
import com.rocketmq.remoting.netty.NettyClientConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * @author xuleyan
 * @version MQClientInstance.java, v 0.1 2020-12-15 10:52 上午
 */
@Slf4j
public class MQClientInstance {
    private final static long LOCK_TIMEOUT_MILLIS = 3000;
    private final ClientConfig clientConfig;
    private final int instanceIndex;
    private final String clientId;
    private final long bootTimestamp = System.currentTimeMillis();
    private final Lock lockHeartbeat = new ReentrantLock();
    private final AtomicLong storeTimesTotal = new AtomicLong(0);
    private final MQClientAPIImpl mQClientAPIImpl;
    private final MQAdminImpl mQAdminImpl;
    private final ClientRemotingProcessor clientRemotingProcessor;
    private final PullMessageService pullMessageService;
    private final RebalanceService rebalanceService;
    private final Lock lockNamesrv = new ReentrantLock();

    private ServiceState serviceState = ServiceState.CREATE_JUST;
    /**
     * client内部producer
     * 目前用于 consumer 发回消息
     */
    private final DefaultMQProducer defaultMQProducer;
    /**
     * Broker名字 和 Broker地址相关 Map,定期(30S)移除关闭了的 broker address
     */
    private final ConcurrentHashMap<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address */>> brokerAddrTable =
            new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String/* topic */, TopicRouteData> topicRouteTable = new ConcurrentHashMap<>();
    /**
     * Producer Map
     */
    private final ConcurrentHashMap<String/* group */, MQProducerInner> producerTable = new ConcurrentHashMap<>();
    /**
     * Consumer Map
     */
    private final ConcurrentHashMap<String/* group */, MQConsumerInner> consumerTable = new ConcurrentHashMap<>();
    private final NettyClientConfig nettyClientConfig;
    private final ConsumerStatsManager consumerStatsManager;

    /**
     * 定时器
     * 目前有如下任务：
     * 1.
     * 2. 定时拉取 Topic路由配置
     * 3.
     * 4.
     */
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "MQClientFactoryScheduledThread");
        }
    });

    public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId, RPCHook rpcHook) {
        this.clientConfig = clientConfig;
        this.instanceIndex = instanceIndex;
        this.clientId = clientId;
        this.nettyClientConfig = new NettyClientConfig();

        log.info("created a new client Instance, FactoryIndex: {} ClinetID: {} {} {}, serializeType={}", //
                this.instanceIndex, //
                this.clientId, //
                this.clientConfig, //
                MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION), RemotingCommand.getSerializeTypeConfigInThisServer());

        this.clientRemotingProcessor = new ClientRemotingProcessor(this);
        this.mQClientAPIImpl = new MQClientAPIImpl(this.nettyClientConfig, this.clientRemotingProcessor, rpcHook, clientConfig);
        if (this.clientConfig.getNamesrvAddr() != null) {
            this.mQClientAPIImpl.updateNameServerAddressList(this.clientConfig.getNamesrvAddr());
            log.info("user specified name server address: {}", this.clientConfig.getNamesrvAddr());
        }

        this.mQAdminImpl = new MQAdminImpl(this);
        this.pullMessageService = new PullMessageService(this);
        this.rebalanceService = new RebalanceService(this);

        this.defaultMQProducer = new DefaultMQProducer(MixAll.CLIENT_INNER_PRODUCER_GROUP);
        this.defaultMQProducer.resetClientConfig(clientConfig);

        this.consumerStatsManager = new ConsumerStatsManager(this.scheduledExecutorService);
        log.info("created a new client Instance, FactoryIndex: {} ClinetID: {} {} {}, serializeType={}", //
                this.instanceIndex, //
                this.clientId, //
                this.clientConfig, //
                MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION), RemotingCommand.getSerializeTypeConfigInThisServer());
    }

    /**
     * 注册Producer
     * 若之前创建过，则返回失败；否则，成功。
     *
     * @param group    分组
     * @param producer producer
     * @return 是否成功。
     */
    public boolean registerProducer(final String group, final DefaultMQProducerImpl producer) {
        if (null == group || null == producer) {
            return false;
        }

        MQProducerInner prev = this.producerTable.putIfAbsent(group, producer);
        if (prev != null) {
            log.warn("the producer group[{}] exist already.", group);
            return false;
        }

        return true;
    }

    public void sendHeartbeatToAllBrokerWithLock() {
        if (this.lockHeartbeat.tryLock()) {
            try {
                this.sendHeartbeatToAllBroker();
            } catch (final Exception e) {
                log.error("sendHeartbeatToAllBroker exception", e);
            } finally {
                this.lockHeartbeat.unlock();
            }
        } else {
            log.warn("lock heartBeat, but failed.");
        }
    }

    /**
     * 向所有Broker发送心跳,被Namesrv关闭连接的不在其中
     * 生产者只向Master发送心跳,因为只有Master才能写入数据
     * 消费者向Master和Slave都发送心跳
     */
    private void sendHeartbeatToAllBroker() {
        final HeartbeatData heartbeatData = this.prepareHeartbeatData();
        boolean producerEmpty = heartbeatData.getProducerDataSet().isEmpty();
        final boolean consumerEmpty = heartbeatData.getConsumerDataSet().isEmpty();
        if (producerEmpty && consumerEmpty) {
            log.warn("sending heartbeat, but no consumer and no producer");
            return;
        }

        long times = this.storeTimesTotal.getAndIncrement();
        Iterator<Map.Entry<String, HashMap<Long, String>>> it = this.brokerAddrTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, HashMap<Long, String>> entry = it.next();
            String brokerName = entry.getKey();
            HashMap<Long, String> oneTable = entry.getValue();
            if (oneTable != null) {
                for (Map.Entry<Long, String> entry1 : oneTable.entrySet()) {
                    Long id = entry1.getKey();
                    String addr = entry1.getValue();
                    if (addr != null) {
                        if (consumerEmpty) {  //没有消费者,当前为纯生产者客户端
                            if (id != MixAll.MASTER_ID) {   //生产者只向Master发送心跳
                                continue;
                            }
                        }

                        try {
                            this.mQClientAPIImpl.sendHearbeat(addr, heartbeatData, 10000000);
                            if (times % 20 == 0) {
                                log.info("send heart beat to broker[{} {} {}] success", brokerName, id, addr);
                                log.info(heartbeatData.toString());
                            }
                        } catch (Exception e) {
                            if (this.isBrokerInNameServer(addr)) {
                                log.error("send heart beat to broker exception", e);
                            } else {
                                log.info("send heart beat to broker[{} {} {}] exception, because the broker not up, forget it", brokerName,
                                        id, addr);
                            }
                        }
                    }
                }
            }
        }

    }

    private boolean isBrokerInNameServer(final String brokerAddr) {
        Iterator<Map.Entry<String, TopicRouteData>> it = this.topicRouteTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, TopicRouteData> itNext = it.next();
            List<BrokerData> brokerDatas = itNext.getValue().getBrokerDatas();
            for (BrokerData brokerData : brokerDatas) {
                if (brokerData.getBrokerAddrs().containsValue(brokerAddr)) {
                    return true;
                }
            }
        }

        return false;
    }

    private HeartbeatData prepareHeartbeatData() {
        HeartbeatData heartbeatData = new HeartbeatData();
        heartbeatData.setClientID(clientId);

        for (Map.Entry<String, MQProducerInner> entry : this.producerTable.entrySet()) {
            MQProducerInner value = entry.getValue();
            if (value != null) {
                ProducerData producerData = new ProducerData();
                producerData.setGroupName(entry.getKey());
                heartbeatData.getProducerDataSet().add(producerData);
            }
        }

        return heartbeatData;
    }

    public void start() throws MQClientException {
        synchronized (this) {
            switch (this.serviceState) {
                case CREATE_JUST:
                    this.serviceState = ServiceState.START_FAILED;
                    // If not specified,looking address from name server
                    if (null == this.clientConfig.getNamesrvAddr()) {
                        //如果url未指定，可以通过Http请求从其他处获取
                        this.mQClientAPIImpl.fetchNameServerAddr();
                    }
                    this.mQClientAPIImpl.start();
                    // 启动多个定时任务
                    this.startScheduledTask();

                    break;
                case RUNNING:
                    break;
                case SHUTDOWN_ALREADY:
                    break;
                case START_FAILED:
                    throw new MQClientException("The Factory object[" + this.getClientId() + "] has been created before, and failed.", null);
                default:
                    break;
            }
        }
    }

    private void startScheduledTask() {
        if (this.clientConfig.getNamesrvAddr() == null) {
            this.clientConfig.setNamesrvAddr("127.0.0.1:9876");
        }

        MQClientInstance.this.updateTopicRouteInfoFromNameServer();

//        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
//            @Override
//            public void run() {
//                try {
//                    MQClientInstance.this.updateTopicRouteInfoFromNameServer();
//                } catch (Exception e) {
//                    log.error("ScheduledTask updateTopicRouteInfoFromNameServer exception", e);
//                }
//            }
//        }, 10, this.clientConfig.getPollNameServerInteval(), TimeUnit.MILLISECONDS);
    }

    private void updateTopicRouteInfoFromNameServer() {
        log.info("updateTopicRouteInfoFromNameServer >> start");
        Set<String> topicList = new HashSet<>();
        // Consumer 获取topic数组
        {

        }

        // Producer 获取topic数组
        {
            Iterator<Map.Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, MQProducerInner> entry = it.next();
                MQProducerInner impl = entry.getValue();
                if (impl != null) {
                    Set<String> lst = impl.getPublishTopicList();
                    topicList.addAll(lst);
                }
            }
        }

        for (String topic : topicList) {
            this.updateTopicRouteInfoFromNameServer(topic);
        }
    }

    /**
     * 更新单个 Topic 路由信息
     *
     * @param topic Topic
     * @return 是否更新成功
     */
    public boolean updateTopicRouteInfoFromNameServer(final String topic) {
        return updateTopicRouteInfoFromNameServer(topic, false, null);
    }

    /**
     * 更新单个 Topic 路由信息
     * 若 isDefault=true && defaultMQProducer!=null 时，使用{@link DefaultMQProducer#createTopicKey}
     *
     * @param topic             Topic
     * @param isDefault         是否默认
     * @param defaultMQProducer producer
     * @return 是否更新成功
     */
    public boolean updateTopicRouteInfoFromNameServer(final String topic, boolean isDefault, DefaultMQProducer defaultMQProducer) {
        try {
            if (this.lockNamesrv.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    TopicRouteData topicRouteData;
                    topicRouteData = this.mQClientAPIImpl.getTopicRouteInfoFromNameServer(topic, 1000 * 100);
                    if (topicRouteData != null) {
                        TopicRouteData old = this.topicRouteTable.get(topic);
                        boolean changed = topicRouteDataIsChange(old, topicRouteData);
                        if (!changed) {
                            changed = this.isNeedUpdateTopicRouteInfo(topic);
                        } else {
                            log.info("the topic[{}] route info changed, old[{}] ,new[{}]", topic, old, topicRouteData);
                        }

                        if (changed) {
                            TopicRouteData cloneTopicRouteData = topicRouteData.cloneTopicRouteData();
                            for (BrokerData brokerData : topicRouteData.getBrokerDatas()) {
                                this.brokerAddrTable.put(brokerData.getBrokerName(), brokerData.getBrokerAddrs());
                            }
                            // 更新生产者里的TopicPublishInfo,Slave在注册Broker时不会生成QueueData,但会生成BrokerData
                            TopicPublishInfo publishInfo = topicRouteData2TopicPublishInfo(topic, topicRouteData);
                            publishInfo.setHaveTopicRouterInfo(true);
                            for (Map.Entry<String, MQProducerInner> entry : this.producerTable.entrySet()) {
                                MQProducerInner impl = entry.getValue();
                                if (impl != null) {
                                    impl.updateTopicPublishInfo(topic, publishInfo);
                                }
                            }

                            // @TODO 更新订阅者(消费者)里的队列信息,Slave在注册Broker时不会生成QueueData,但会生成BrokerData

                            log.info("topicRouteTable.put TopicRouteData[{}]", cloneTopicRouteData);
                            this.topicRouteTable.put(topic, cloneTopicRouteData);
                            return true;
                        }
                    }
                } catch (Exception ex) {
                    log.warn("updateTopicRouteInfoFromNameServer, getTopicRouteInfoFromNameServer return null, Topic: {}", topic);
                }
            } else {
                log.warn("updateTopicRouteInfoFromNameServer tryLock timeout {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (Exception ex) {
            log.warn("updateTopicRouteInfoFromNameServer Exception", ex);
        } finally {
            this.lockNamesrv.unlock();
        }
        return false;
    }

    /**
     * 将 Topic路由数据 转换成 Topic发布信息，过滤Master挂了的Broker以及Slave的MessageQueue
     * 顺序消息
     * 非顺序消息
     *
     * @param topic Topic
     * @param route Topic路由数据
     * @return Topic信息
     */
    private TopicPublishInfo topicRouteData2TopicPublishInfo(final String topic, final TopicRouteData route) {
        TopicPublishInfo info = new TopicPublishInfo();
        info.setTopicRouteData(route);
        if (route.getOrderTopicConf() != null && route.getOrderTopicConf().length() > 0) {
            info.setOrderTopic(true);
            // 发送顺序消息
        } else {
            List<QueueData> qds = route.getQueueDatas();
            Collections.sort(qds);
            for (QueueData qd : qds) {
                if (PermName.isWriteable(qd.getPerm())) {
                    BrokerData brokerData = null;
                    for (BrokerData bd : route.getBrokerDatas()) {
                        if (bd.getBrokerName().equals(qd.getBrokerName())) {
                            brokerData = bd;
                            break;
                        }
                    }

                    if (null == brokerData) {
                        continue;
                    }

                    // 若BrokerData不包含Master节点地址，可能Master已经挂了，所以不处理消息
                    if (!brokerData.getBrokerAddrs().containsKey(MixAll.MASTER_ID)) {
                        continue;
                    }

                    // 创建队列信息，只有那些经过校验的QueueData才能创建队列信息
                    for (int i = 0; i < qd.getWriteQueueNums(); i++) {
                        MessageQueue messageQueue = new MessageQueue(topic, qd.getBrokerName(), i);
                        info.getMessageQueueList().add(messageQueue);
                    }
                }
            }
            info.setOrderTopic(false);
        }
        return info;
    }

    private boolean isNeedUpdateTopicRouteInfo(String topic) {
        boolean result = false;
        {
            Iterator<Map.Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
            while (it.hasNext() && !result) {
                Map.Entry<String, MQProducerInner> entry = it.next();
                MQProducerInner impl = entry.getValue();
                if (impl != null) {
                    result = impl.isPublishTopicNeedUpdate(topic);
                }
            }
        }
        return result;
    }

    private boolean topicRouteDataIsChange(TopicRouteData oldData, TopicRouteData newData) {
        if (oldData == null || newData == null) {
            return true;
        }
        TopicRouteData old = oldData.cloneTopicRouteData();
        TopicRouteData now = newData.cloneTopicRouteData();
        Collections.sort(old.getBrokerDatas());
        Collections.sort(old.getQueueDatas());
        Collections.sort(now.getQueueDatas());
        Collections.sort(now.getBrokerDatas());
        return !old.equals(now);
    }

    public String getClientId() {
        return clientId;
    }
}