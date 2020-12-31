/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.common.protocol.header.namesrv;

/**
 *
 * @author xuleyan
 * @version GetRouteInfoRequestHeader.java, v 0.1 2020-12-17 11:39 上午
 */

import com.rocketmq.remoting.CommandCustomHeader;
import com.rocketmq.remoting.annotations.CFNotNull;
import com.rocketmq.remoting.exception.RemotingCommandException;

public class GetRouteInfoRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String topic;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
