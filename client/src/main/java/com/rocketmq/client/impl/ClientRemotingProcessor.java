/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.client.impl;

import com.rocketmq.client.impl.factory.MQClientInstance;
import com.rocketmq.remoting.RemotingCommand;
import com.rocketmq.remoting.exception.RemotingCommandException;
import com.rocketmq.remoting.netty.NettyRequestProcessor;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author xuleyan
 * @version ClientRemotingProcessor.java, v 0.1 2020-12-15 9:05 下午
 */
@Slf4j
public class ClientRemotingProcessor implements NettyRequestProcessor {

    private final MQClientInstance mqClientFactory;

    public ClientRemotingProcessor(final MQClientInstance mqClientFactory) {
        this.mqClientFactory = mqClientFactory;
    }


    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        return null;
    }
}