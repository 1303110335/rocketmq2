package com.rocketmq.namesrv; /**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */

import com.rocketmq.remoting.netty.NettyServerConfig;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author xuleyan
 * @version com.rocketmq.namesrv.NamesrvStart.java, v 0.1 2020-12-03 3:30 下午
 */
@Slf4j
public class NamesrvStart {

    public static void main(String[] args) {

        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setServerWorkerThreads(1);
        nettyServerConfig.setListenPort(9876);
        nettyServerConfig.setServerChannelMaxIdleTimeSeconds(60);

        NamesrvController namesrvController = new NamesrvController(nettyServerConfig);
        namesrvController.init();
        namesrvController.start();
    }
}