/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * $Id: SendMessageRequestHeader.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package com.rocketmq.common.protocol.header;

import com.rocketmq.remoting.CommandCustomHeader;
import com.rocketmq.remoting.annotations.CFNotNull;
import com.rocketmq.remoting.annotations.CFNullable;
import com.rocketmq.remoting.exception.RemotingCommandException;

/**
 * 发送消息请求Header
 */
public class SendMessageRequestHeader implements CommandCustomHeader {
    /**
     * producer分组
     */
    @CFNotNull
    private String producerGroup;
    /**
     * Topic
     */
    @CFNotNull
    private String topic;
    /**
     * 默认Topic
     */
    @CFNotNull
    private String defaultTopic;
    /**
     * 默认Topic消息队列数量
     */
    @CFNotNull
    private Integer defaultTopicQueueNums;
    /**
     * 消息队列编号
     */
    @CFNotNull
    private Integer queueId;
    /**
     * 消息系统标记
     */
    @CFNotNull
    private Integer sysFlag;
    /**
     * 创建时间
     */
    @CFNotNull
    private Long bornTimestamp;
    @CFNotNull
    private Integer flag;
    /**
     * 拓展字段
     */
    @CFNullable
    private String properties;
    /**
     * 已消费次数
     */
    @CFNullable
    private Integer reconsumeTimes;
    // TODO 疑问：unitMode是？
    @CFNullable
    private boolean unitMode = false;
    /**
     * 最大消费次数,可自定义
     */
    private Integer maxReconsumeTimes;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getDefaultTopic() {
        return defaultTopic;
    }

    public void setDefaultTopic(String defaultTopic) {
        this.defaultTopic = defaultTopic;
    }

    public Integer getDefaultTopicQueueNums() {
        return defaultTopicQueueNums;
    }

    public void setDefaultTopicQueueNums(Integer defaultTopicQueueNums) {
        this.defaultTopicQueueNums = defaultTopicQueueNums;
    }

    public Integer getQueueId() {
        return queueId;
    }

    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }

    public Integer getSysFlag() {
        return sysFlag;
    }

    public void setSysFlag(Integer sysFlag) {
        this.sysFlag = sysFlag;
    }

    public Long getBornTimestamp() {
        return bornTimestamp;
    }

    public void setBornTimestamp(Long bornTimestamp) {
        this.bornTimestamp = bornTimestamp;
    }

    public Integer getFlag() {
        return flag;
    }

    public void setFlag(Integer flag) {
        this.flag = flag;
    }

    public String getProperties() {
        return properties;
    }

    public void setProperties(String properties) {
        this.properties = properties;
    }

    public Integer getReconsumeTimes() {
        return reconsumeTimes;
    }

    public void setReconsumeTimes(Integer reconsumeTimes) {
        this.reconsumeTimes = reconsumeTimes;
    }

    public boolean isUnitMode() {
        return unitMode;
    }

    public void setUnitMode(boolean isUnitMode) {
        this.unitMode = isUnitMode;
    }

    public Integer getMaxReconsumeTimes() {
        return maxReconsumeTimes;
    }

    public void setMaxReconsumeTimes(final Integer maxReconsumeTimes) {
        this.maxReconsumeTimes = maxReconsumeTimes;
    }
}
