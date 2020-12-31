/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.namesrv.routeinfo;

import com.rocketmq.common.DataVersion;
import com.rocketmq.common.MixAll;
import com.rocketmq.common.namesrv.RegisterBrokerResult;
import com.rocketmq.common.protocol.TopicConfig;
import com.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import com.rocketmq.common.protocol.route.BrokerData;
import com.rocketmq.common.protocol.route.QueueData;
import com.rocketmq.common.protocol.route.TopicRouteData;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 *
 * @author xuleyan
 * @version RouteInfoManager.java, v 0.1 2020-12-09 2:45 下午
 */
@Slf4j
public class RouteInfoManager {

    private final static long BROKER_CHANNEL_EXPIRED_TIME = 1000 * 60 * 2;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    /**
     * topic 与 队列数据数组 Map
     * 一个 topic 可以对应 多个Broker
     */
    private final HashMap<String/* topic */, List<QueueData>> topicQueueTable;
    /**
     * broker名 与 broker数据 Map
     * 一个broker名下可以有多台机器
     */
    private final HashMap<String/* brokerName */, BrokerData> brokerAddrTable;
    /**
     * 集群 与 broker集合 Map
     */
    private final HashMap<String/* clusterName */, Set<String/* brokerName */>> clusterAddrTable;
    /**
     * broker地址 与 broker连接信息 Map
     */
    private final HashMap<String/* brokerAddr */, BrokerLiveInfo> brokerLiveTable;

    public RouteInfoManager() {
        // TODO 疑问：为什么初始化选择了这些参数
        this.topicQueueTable = new HashMap<String, List<QueueData>>(1024);
        this.brokerAddrTable = new HashMap<String, BrokerData>(128);
        this.clusterAddrTable = new HashMap<String, Set<String>>(32);
        this.brokerLiveTable = new HashMap<String, BrokerLiveInfo>(256);
        //this.filterServerTable = new HashMap<String, List<String>>(256);
    }

    /**
     * 当超过2M没有心跳或者是心跳连接断开,关闭Broker的长连接,
     * 长时间没有发送心跳可能是应用出现问题,心跳断开可能是服务器IO出现问题
     * 移除相应的{@link #brokerLiveTable}
     * 移除{@link #brokerAddrTable}里相应{@link BrokerData#brokerAddrs}
     * 如果当前BrokerData不存在Broker时，移除此BrokerData
     *
     * @param remoteAddr
     * @param channel
     */
    public void onChannelDestroy(String remoteAddr, Channel channel) {
        String brokerAddrFound = null;
        if (channel != null) {
            try {
                this.lock.readLock().lockInterruptibly();
                Iterator<Map.Entry<String, BrokerLiveInfo>> itBrokerLiveTable = this.brokerLiveTable.entrySet().iterator();
                while (itBrokerLiveTable.hasNext()) {
                    Map.Entry<String, BrokerLiveInfo> entry = itBrokerLiveTable.next();
                    if (entry.getValue().getChannel() == channel) {
                        brokerAddrFound = entry.getKey();
                        break;
                    }
                }

            } catch (InterruptedException e) {
                log.error("onChannelDestroy Exception", e);
            } finally {
                this.lock.readLock().unlock();
            }
        }

        if (null == brokerAddrFound) {
            brokerAddrFound = remoteAddr;
        } else {
            log.info("the broker's channel destroyed, {}, clean it's data structure at once", brokerAddrFound);
        }

        if (brokerAddrFound != null && brokerAddrFound.length() > 0) {
            try {
                this.lock.writeLock().lockInterruptibly();
                this.brokerLiveTable.remove(brokerAddrFound);
                String brokerNameFound = null;
                boolean removeBrokerName = false;
                Iterator<Map.Entry<String, BrokerData>> itBrokerAddrTable = this.brokerAddrTable.entrySet().iterator();
                while (itBrokerAddrTable.hasNext() && (null == brokerNameFound)) {
                    BrokerData brokerData = itBrokerAddrTable.next().getValue();
                    Iterator<Map.Entry<Long, String>> it = brokerData.getBrokerAddrs().entrySet().iterator();
                    while (it.hasNext()) {
                        Map.Entry<Long, String> entry = it.next();
                        Long brokerId = entry.getKey();
                        String brokerAddr = entry.getValue();
                        if (brokerAddr.equals(brokerAddrFound)) {
                            brokerNameFound = brokerData.getBrokerName();
                            it.remove();
                            log.info("remove brokerAddr[{}, {}] from brokerAddrTable, because channel destroyed", brokerId, brokerAddr);
                            break;
                        }
                    }

                    if (brokerData.getBrokerAddrs().isEmpty()) {
                        removeBrokerName = true;
                        itBrokerAddrTable.remove();
                        log.info("remove brokerName[{}] from brokerAddrTable, because channel destroyed", brokerData.getBrokerName());
                    }
                }

                if (brokerNameFound != null && removeBrokerName) {
                    Iterator<Map.Entry<String, Set<String>>> it = this.clusterAddrTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Map.Entry<String, Set<String>> entry = it.next();
                        String clusterName = entry.getKey();
                        Set<String> brokerNames = entry.getValue();
                        boolean removed = brokerNames.remove(brokerNameFound);
                        if (removed) {
                            log.info("remove brokerName[{}], clusterName[{}] from clusterAddrTable, because channel destroyed", brokerNameFound, clusterName);

                            if (brokerNames.isEmpty()) {
                                log.info("remove the clusterName[{}] from clusterAddrTable, because channel destroyed and no broker in this cluster", clusterName);
                                it.remove();
                            }

                            break;
                        }
                    }

                    if (removeBrokerName) {
                        Iterator<Map.Entry<String, List<QueueData>>> itTopicQueueTable = this.topicQueueTable.entrySet().iterator();
                        while (itTopicQueueTable.hasNext()) {
                            Map.Entry<String, List<QueueData>> entry = itTopicQueueTable.next();
                            String topic = entry.getKey();
                            List<QueueData> queueDataList = entry.getValue();

                            Iterator<QueueData> itQueueData = queueDataList.iterator();
                            while (itQueueData.hasNext()) {
                                QueueData queueData = itQueueData.next();
                                if (queueData.getBrokerName().equals(brokerNameFound)) {
                                    itQueueData.remove();
                                    log.info("remove topic[{} {}], from topicQueueTable, because channel destroyed", topic, queueData);
                                }
                            }
                            if (queueDataList.isEmpty()) {
                                itTopicQueueTable.remove();
                                log.info("remove topic[{}] all queue, from topicQueueTable, because channel destroyed", topic);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                log.error("onChannelDestroy Exception", e);
            } finally {
                this.lock.writeLock().unlock();
            }
        }

    }

    /**
     * broker 注册
     *
     * @param clusterName        集群名
     * @param brokerAddr         broker地址
     * @param brokerName         broker名
     * @param brokerId           broker角色编号
     * @param haServerAddr       TODO
     * @param topicConfigWrapper topic配置数组
     * @param filterServerList   filtersrv数组
     * @param channel            连接channel
     * @return broker注册结果。
     * 当 broker 为主节点时：不返回 haServerAddr、masterAddr
     * 当 broker 为从节点时：返回 haServerAddr、masterAddr
     */
    public RegisterBrokerResult registerBroker(
            final String clusterName,
            final String brokerAddr,
            final String brokerName,
            final long brokerId,
            final String haServerAddr,
            final TopicConfigSerializeWrapper topicConfigWrapper,
            final List<String> filterServerList,
            final Channel channel) {

        RegisterBrokerResult result = new RegisterBrokerResult();
        try {
            this.lock.writeLock().lockInterruptibly();
            Set<String> brokerNames = this.clusterAddrTable.get(clusterName);
            if (null == brokerNames) {
                brokerNames = new HashSet<String>();
                this.clusterAddrTable.put(clusterName, brokerNames);
            }
            brokerNames.add(brokerName);
            log.info("registerBroker >> clusterAddrTable = {}", clusterAddrTable);

            // 是不是注册某个Broker下新的机器,Master不一定要比Slave早注册
            boolean registerFirst = false;
            BrokerData brokerData = this.brokerAddrTable.get(brokerName);
            if (null == brokerData) {
                registerFirst = true;
                brokerData = new BrokerData();
                brokerData.setBrokerName(brokerName);
                HashMap<Long, String> brokerAddrs = new HashMap<Long, String>();
                brokerData.setBrokerAddrs(brokerAddrs);
                this.brokerAddrTable.put(brokerName, brokerData);
            }
            String oldAddr = brokerData.getBrokerAddrs().put(brokerId, brokerAddr);
            log.info("registerBroker >> brokerAddrTable = {}", brokerAddrTable);
            // 当前Broker的第一台机器 || 当前Broker下新的机器
            registerFirst = registerFirst || (null == oldAddr);

            // 当注册Master && 有Topic配置时,更新topic信息
            if (null != topicConfigWrapper && MixAll.MASTER_ID == brokerId) {
                // 当这次的心跳是Master发送的, 且Topic配置信息有变化 || Master的第一次心跳
                if (this.isBrokerTopicConfigChanged(brokerAddr, topicConfigWrapper.getDataVersion()) || registerFirst) {
                    ConcurrentHashMap<String, TopicConfig> tcTable = topicConfigWrapper.getTopicConfigTable();
                    if (tcTable != null) {
                        for (Map.Entry<String, TopicConfig> entry : tcTable.entrySet()) {
                            //只有Master相关的注册才会创建QueueData
                            this.createAndUpdateQueueData(brokerName, entry.getValue());
                        }
                    }
                }
            }

            // 更新broker连接信息
            BrokerLiveInfo prevBrokerLiveInfo = this.brokerLiveTable.put(brokerAddr,
                    new BrokerLiveInfo(
                            System.currentTimeMillis(),
                            topicConfigWrapper.getDataVersion(),
                            channel,
                            haServerAddr));
            log.info("registerBroker >> brokerLiveTable = {}", brokerLiveTable);
            if (null == prevBrokerLiveInfo) {
                log.info("new broker registerd, {} HAServer: {}", brokerAddr, haServerAddr);
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            this.lock.writeLock().unlock();
        }
        return result;
    }

    private void createAndUpdateQueueData(String brokerName, final TopicConfig topicConfig) {
        QueueData queueData = new QueueData();
        queueData.setBrokerName(brokerName);
        queueData.setWriteQueueNums(topicConfig.getWriteQueueNums());
        queueData.setReadQueueNums(topicConfig.getReadQueueNums());
        queueData.setPerm(topicConfig.getPerm());
        queueData.setTopicSynFlag(topicConfig.getTopicSysFlag());

        List<QueueData> queueDataList = this.topicQueueTable.get(topicConfig.getTopicName());
        if (queueDataList == null) {
            queueDataList = new LinkedList<QueueData>();
            queueDataList.add(queueData);
            this.topicQueueTable.put(topicConfig.getTopicName(), queueDataList);
            log.info("new topic registered, {} {}", topicConfig.getTopicName(), queueData);
        } else {
            boolean addNewOne = true;

            Iterator<QueueData> it = queueDataList.iterator();
            while (it.hasNext()) {
                QueueData qd = it.next();
                if (qd.getBrokerName().equals(brokerName)) {
                    if (qd.equals(queueData)) {    //原始QueueData是否和新建的一样
                        addNewOne = false;
                    } else {
                        log.info("topic changed, {} OLD: {} NEW: {}", topicConfig.getTopicName(), qd,
                                queueData);
                        it.remove();
                    }
                }
            }

            if (addNewOne) {
                queueDataList.add(queueData);
            }
        }


    }

    /**
     * 是否Broker Topic配置有变化
     *
     * @param brokerAddr  broker地址
     * @param dataVersion 数据版本
     * @return 是否变化
     */
    private boolean isBrokerTopicConfigChanged(final String brokerAddr, final DataVersion dataVersion) {
        BrokerLiveInfo prev = this.brokerLiveTable.get(brokerAddr);
        if (null == prev || !prev.getDataVersion().equals(dataVersion)) {
            return true;
        }

        return false;
    }

    /**
     * 从{@link #topicQueueTable}里获取指定List<QueueData>
     * 从{@link #brokerAddrTable}里获取BrokerData
     * 组合成TopicRouteData返回给客户端
     *
     * @param topic
     * @return
     */
    public TopicRouteData pickupTopicRouteData(final String topic) {
        TopicRouteData topicRouteData = new TopicRouteData();
        boolean foundQueueData = false;
        boolean foundBrokerData = false;
        Set<String> brokerNameSet = new HashSet<>();
        List<BrokerData> brokerDataList = new LinkedList<>();
        topicRouteData.setBrokerDatas(brokerDataList);

        try {
            this.lock.readLock().lockInterruptibly();
            List<QueueData> queueDataList = this.topicQueueTable.get(topic);
            if (queueDataList != null) {
                topicRouteData.setQueueDatas(queueDataList);
                foundQueueData = true;
            } else {
                queueDataList = new ArrayList<>();
            }

            for (QueueData queueData : queueDataList) {
                brokerNameSet.add(queueData.getBrokerName());
            }

            for (String brokerName : brokerNameSet) {
                BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                if (brokerData != null) {
                    BrokerData brokerDataClone = new BrokerData();
                    brokerDataClone.setBrokerName(brokerData.getBrokerName());
                    brokerDataClone.setBrokerAddrs((HashMap<Long, String>) brokerData.getBrokerAddrs().clone());
                    brokerDataList.add(brokerDataClone);
                    foundBrokerData = true;
                }
            }
        } catch (InterruptedException e) {
            log.error("pickupTopicRouteData Exception", e);
        } finally {
            this.lock.readLock().unlock();
        }
        log.debug("pickupTopicRouteData {} {}", topic, topicRouteData);
        if (foundQueueData && foundBrokerData) {
            return topicRouteData;
        }
        return null;
    }
}


/**
 * Broker 连接信息
 */
class BrokerLiveInfo {

    /**
     * 最后更新时间
     */
    private long lastUpdateTimestamp;
    /**
     * 数据版本号
     */
    private DataVersion dataVersion;
    /**
     * 连接信息
     */
    private Channel channel;
    /**
     * ha服务器地址
     */
    private String haServerAddr;

    public BrokerLiveInfo(long lastUpdateTimestamp, DataVersion dataVersion, Channel channel,
                          String haServerAddr) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
        this.dataVersion = dataVersion;
        this.channel = channel;
        this.haServerAddr = haServerAddr;
    }

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(DataVersion dataVersion) {
        this.dataVersion = dataVersion;
    }

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    public String getHaServerAddr() {
        return haServerAddr;
    }

    public void setHaServerAddr(String haServerAddr) {
        this.haServerAddr = haServerAddr;
    }

    @Override
    public String toString() {
        return "BrokerLiveInfo [lastUpdateTimestamp=" + lastUpdateTimestamp + ", dataVersion=" + dataVersion
                + ", channel=" + channel + ", haServerAddr=" + haServerAddr + "]";
    }
}