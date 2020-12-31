/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.remoting;

import com.rocketmq.remoting.netty.NettyRequestProcessor;

import java.nio.channels.Channel;
import java.util.concurrent.ExecutorService;

/**
 * @author xuleyan
 * @version RemotingServer.java, v 0.1 2020-10-13 9:34 下午
 */
public interface RemotingServer extends RemotingService {

    void registerProcessor(final int requestCode, final NettyRequestProcessor processor,
                           final ExecutorService executor);

    RemotingCommand invokeSync(final Channel channel, final RemotingCommand request, final long timeoutMillis);

    void invokeAsync(final Channel channel, final RemotingCommand request, final long timeoutMillis,
                     final InvokeCallback invokeCallback);

    void invokeOneway(final Channel channel, final RemotingCommand request, final long timeoutMillis);

    void registerDefaultProcessor(final NettyRequestProcessor processor, final ExecutorService executor);

}