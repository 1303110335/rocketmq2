/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.common.protocol.body;

import com.rocketmq.common.DataVersion;
import com.rocketmq.common.protocol.TopicConfig;
import com.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author xuleyan
 * @version TopicConfigSerializeWrapper.java, v 0.1 2020-10-11 12:12 下午
 */
public class TopicConfigSerializeWrapper extends RemotingSerializable {

    private ConcurrentHashMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>();

    private DataVersion dataVersion = new DataVersion();

    public ConcurrentHashMap<String, TopicConfig> getTopicConfigTable() {
        return topicConfigTable;
    }

    public void setTopicConfigTable(ConcurrentHashMap<String, TopicConfig> topicConfigTable) {
        this.topicConfigTable = topicConfigTable;
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(DataVersion dataVersion) {
        this.dataVersion = dataVersion;
    }
}