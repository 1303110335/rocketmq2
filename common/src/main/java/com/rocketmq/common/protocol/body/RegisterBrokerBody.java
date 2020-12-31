/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.common.protocol.body;

import com.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xuleyan
 * @version RegisterBrokerBody.java, v 0.1 2020-10-11 12:01 下午
 */
public class RegisterBrokerBody extends RemotingSerializable {

    private TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();

    private List<String> filterServerList = new ArrayList<String>();

    public TopicConfigSerializeWrapper getTopicConfigSerializeWrapper() {
        return topicConfigSerializeWrapper;
    }

    public void setTopicConfigSerializeWrapper(TopicConfigSerializeWrapper topicConfigSerializeWrapper) {
        this.topicConfigSerializeWrapper = topicConfigSerializeWrapper;
    }

    public List<String> getFilterServerList() {
        return filterServerList;
    }

    public void setFilterServerList(List<String> filterServerList) {
        this.filterServerList = filterServerList;
    }
}