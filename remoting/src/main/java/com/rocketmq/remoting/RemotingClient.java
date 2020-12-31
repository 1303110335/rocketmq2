/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.remoting;

import com.rocketmq.remoting.exception.RemotingConnectException;
import com.rocketmq.remoting.exception.RemotingSendRequestException;
import com.rocketmq.remoting.exception.RemotingTimeoutException;
import com.rocketmq.remoting.netty.NettyRequestProcessor;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 *
 * @author xuleyan
 * @version RemotingClient.java, v 0.1 2020-12-06 10:47 下午
 */
public interface RemotingClient extends RemotingService {

    public RemotingCommand invokeSync(final String addr, final RemotingCommand request,
                                      final long timeoutMillis) throws InterruptedException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException;

    public void invokeAsync(final String addr, final RemotingCommand request, final long timeoutMillis,
                            final InvokeCallback invokeCallback) throws InterruptedException;

    public void invokeOneway(final String addr, final RemotingCommand request, final long timeoutMillis)
            throws InterruptedException;

    public void registerProcessor(final int requestCode, final NettyRequestProcessor processor,
                                  final ExecutorService executor);

    void updateNameServerAddressList(final List<String> addrs);

    List<String> getNameServerAddressList();
}