/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.namesrv.routeinfo;

import com.rocketmq.namesrv.NamesrvController;
import com.rocketmq.remoting.ChannelEventListener;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author xuleyan
 * @version BrokerHousekeepingService.java, v 0.1 2020-12-09 2:43 下午
 */
@Slf4j
public class BrokerHousekeepingService implements ChannelEventListener {

    private final NamesrvController namesrvController;

    public BrokerHousekeepingService(NamesrvController namesrvController) {
        this.namesrvController = namesrvController;
    }

    @Override
    public void onChannelConnect(String remoteAddr, Channel channel) {

    }

    @Override
    public void onChannelClose(String remoteAddr, Channel channel) {
        this.namesrvController.getRouteInfoManager().onChannelDestroy(remoteAddr, channel);
    }

    @Override
    public void onChannelException(String remoteAddr, Channel channel) {
        this.namesrvController.getRouteInfoManager().onChannelDestroy(remoteAddr, channel);
    }

    @Override
    public void onChannelIdle(String remoteAddr, Channel channel) {
        this.namesrvController.getRouteInfoManager().onChannelDestroy(remoteAddr, channel);
    }
}