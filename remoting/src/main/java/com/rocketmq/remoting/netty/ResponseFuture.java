/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.remoting.netty;

import com.rocketmq.remoting.RemotingCommand;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author xuleyan
 * @version ResponseFuture.java, v 0.1 2020-11-29 9:11 下午
 */
@Slf4j
public class ResponseFuture {

    private final int opaque;
    private final long timeoutMillis;
    private volatile boolean sendRequestOK = true;
    private volatile Throwable cause;

    private volatile RemotingCommand responseCommand;

    private final CountDownLatch countDownLatch = new CountDownLatch(1);

    public ResponseFuture(int opaque, long timeoutMillis) {
        this.opaque = opaque;
        this.timeoutMillis = timeoutMillis;
    }

    public RemotingCommand getResponseCommand() {
        return responseCommand;
    }

    public void setResponseCommand(RemotingCommand responseCommand) {
        this.responseCommand = responseCommand;
    }

    public boolean isSendRequestOK() {
        return sendRequestOK;
    }

    public void setSendRequestOK(boolean sendRequestOK) {
        this.sendRequestOK = sendRequestOK;
    }

    public void putResponse(final RemotingCommand remotingCommand) {
        log.info(Thread.currentThread().getName() + "客户端处理返回数据, 来源：channelRead0  >> remotingCommand = {}", remotingCommand);
        this.responseCommand = remotingCommand;
        countDownLatch.countDown();
    }

    public RemotingCommand waitResponse(final long timeoutMills) throws InterruptedException {
        log.info(Thread.currentThread().getName() + "等待客户端返回数据");
        this.countDownLatch.await(timeoutMills, TimeUnit.MILLISECONDS);
        return this.responseCommand;
    }

    public Throwable getCause() {
        return cause;
    }

    public void setCause(Throwable cause) {
        this.cause = cause;
    }
}