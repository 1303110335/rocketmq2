/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.client.impl.producer;

import com.rocketmq.client.common.ThreadLocalIndex;
import com.rocketmq.common.message.MessageQueue;
import com.rocketmq.common.protocol.route.TopicRouteData;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author xuleyan
 * @version TopicPublishInfo.java, v 0.1 2020-12-14 9:51 下午
 */
public class TopicPublishInfo {

    /**
     * 是否顺序消息
     */
    private boolean orderTopic = false;
    /**
     * 是否有路由信息
     */
    private boolean haveTopicRouterInfo = false;
    /**
     * 消息队列数组
     */
    private List<MessageQueue> messageQueueList = new ArrayList<MessageQueue>();
    /**
     * 线程变量（Index）
     */
    private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();
    /**
     * Topic消息路由信息
     */
    private TopicRouteData topicRouteData;

    /**
     * Topic 是否正常：消息队列不为空
     *
     * @return 是否正常
     */
    public boolean ok() {
        return null != this.messageQueueList && !this.messageQueueList.isEmpty();
    }

    public boolean isOrderTopic() {
        return orderTopic;
    }

    public void setOrderTopic(boolean orderTopic) {
        this.orderTopic = orderTopic;
    }

    public boolean isHaveTopicRouterInfo() {
        return haveTopicRouterInfo;
    }

    public void setHaveTopicRouterInfo(boolean haveTopicRouterInfo) {
        this.haveTopicRouterInfo = haveTopicRouterInfo;
    }

    public List<MessageQueue> getMessageQueueList() {
        return messageQueueList;
    }

    public void setMessageQueueList(List<MessageQueue> messageQueueList) {
        this.messageQueueList = messageQueueList;
    }

    public ThreadLocalIndex getSendWhichQueue() {
        return sendWhichQueue;
    }

    public void setSendWhichQueue(ThreadLocalIndex sendWhichQueue) {
        this.sendWhichQueue = sendWhichQueue;
    }

    public TopicRouteData getTopicRouteData() {
        return topicRouteData;
    }

    public void setTopicRouteData(TopicRouteData topicRouteData) {
        this.topicRouteData = topicRouteData;
    }

    public MessageQueue selectOneMessageQueue(final String lastBrokerName) {
        int index = this.sendWhichQueue.getAndIncrement();
        if (lastBrokerName == null) {
            return selectOneMessageQueue();
        } else {
            for (int i = 0; i < this.messageQueueList.size(); i++) {
                int pos = Math.abs(index) % this.messageQueueList.size();
                if (pos < 0) {
                    pos = 0;
                }
                MessageQueue messageQueue = this.messageQueueList.get(index);
                if (messageQueue.getBrokerName().equals(lastBrokerName)) {
                    return messageQueue;
                }
            }
            return selectOneMessageQueue();
        }
    }

    /**
     * 直接选择上次发送队列的下一位
     * @return
     */
    public MessageQueue selectOneMessageQueue() {
        int index = this.sendWhichQueue.getAndIncrement();
        int pos = Math.abs(index) % this.messageQueueList.size();
        if (pos < 0) {
            pos = 0;
        }
        return this.messageQueueList.get(index);
    }
}