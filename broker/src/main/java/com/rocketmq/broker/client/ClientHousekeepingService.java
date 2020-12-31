/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.broker.client;

import com.rocketmq.broker.BrokerController;
import com.rocketmq.remoting.ChannelEventListener;
import io.netty.channel.Channel;

/**
 *
 * @author xuleyan
 * @version ClientHousekeepingService.java, v 0.1 2020-12-10 8:58 下午
 */
public class ClientHousekeepingService implements ChannelEventListener {


    private final BrokerController brokerController;

    public ClientHousekeepingService(BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    @Override
    public void onChannelConnect(String remoteAddr, Channel channel) {

    }

    @Override
    public void onChannelClose(String remoteAddr, Channel channel) {

    }

    @Override
    public void onChannelException(String remoteAddr, Channel channel) {

    }

    @Override
    public void onChannelIdle(String remoteAddr, Channel channel) {

    }
}