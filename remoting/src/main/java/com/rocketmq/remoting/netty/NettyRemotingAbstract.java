/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.remoting.netty;

import com.rocketmq.remoting.ChannelEventListener;
import com.rocketmq.remoting.RemotingCommand;
import com.rocketmq.remoting.RemotingServer;
import com.rocketmq.remoting.common.Pair;
import com.rocketmq.remoting.common.RemotingHelper;
import com.rocketmq.remoting.common.ServiceThread;
import com.rocketmq.remoting.exception.RemotingCommandException;
import com.rocketmq.remoting.exception.RemotingSendRequestException;
import com.rocketmq.remoting.exception.RemotingTimeoutException;
import com.rocketmq.remoting.protocol.RemotingCommandType;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author xuleyan
 * @version RemotingAbstract.java, v 0.1 2020-11-29 7:58 下午
 */
@Slf4j
public abstract class NettyRemotingAbstract implements RemotingServer {


    protected final ConcurrentHashMap<Integer/* opaque*/, ResponseFuture> responseTable = new ConcurrentHashMap<Integer, ResponseFuture>();
    protected final HashMap<Integer, Pair<NettyRequestProcessor, ExecutorService>> processTable = new HashMap<>(64);

    protected Pair<NettyRequestProcessor, ExecutorService> defaultRequestProcessor;
    protected final NettyEventExecuter nettyEventExecuter = new NettyEventExecuter();

    @Override
    public void registerDefaultProcessor(NettyRequestProcessor processor, ExecutorService executor) {
        this.defaultRequestProcessor = new Pair<>(processor, executor);
    }

    public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
        final RemotingCommand cmd = msg;
        log.info("{} 接收到的信息 >> cmd = {}", Thread.currentThread().getName(), cmd);
        if (cmd != null && cmd.getType() != null) {
            switch (cmd.getType()) {
                case REQUEST_COMMAND:
                    // 服务端处理请求数据
                    processRequestCommand(ctx, cmd);
                    break;
                case RESPONSE_COMMAND:
                    // 客户端处理返回数据
                    processResponseCommand(ctx, cmd);
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * 客户端处理返回数据
     *
     * @param ctx
     * @param cmd
     */
    private void processResponseCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
        ResponseFuture responseFuture = responseTable.get(cmd.getOpaque());
        if (responseFuture == null) {
            log.error("responseFuture not found, cmd = {}", cmd);
            return;
        }
        responseFuture.putResponse(cmd);
    }

    /**
     * 服务器端处理请求数据
     *
     * @param ctx
     * @param cmd
     */
    private void processRequestCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
        final Pair<NettyRequestProcessor, ExecutorService> matched = this.processTable.get(cmd.getCode());
        final Pair<NettyRequestProcessor, ExecutorService> pair = matched == null ? defaultRequestProcessor : matched;
        final int opaque = cmd.getOpaque();
        Runnable run = new Runnable() {
            @Override
            public void run() {
                try {
                    RemotingCommand response = pair.getObject1().processRequest(ctx, cmd);
                    response.setOpaque(opaque);
                    response.setType(RemotingCommandType.RESPONSE_COMMAND);
                    try {
                        log.info("返回数据 >> response = {}", response);
                        ctx.writeAndFlush(response);
                    } catch (Throwable e) {
                        log.error("process request over, but response failed", e);
                        log.error(cmd.toString());
                        log.error(response.toString());
                    }
                } catch (RemotingCommandException e) {
                    log.error("request err", e);
                }
            }
        };

        defaultRequestProcessor.getObject2().execute(run);
    }

    protected RemotingCommand invokeSyncImpl(final Channel channel, final RemotingCommand request, final Long timeoutMillis) throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
        final int opaque = request.getOpaque();

        try {
            final ResponseFuture responseFuture = new ResponseFuture(opaque, timeoutMillis);
            this.responseTable.put(opaque, responseFuture);
            SocketAddress addr = channel.remoteAddress();

            log.info("invokeSyncImpl >> 发送数据 >> request = {}", request);
            channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        responseFuture.setSendRequestOK(true);
                        return;
                    } else {
                        responseFuture.setSendRequestOK(false);
                    }
                    responseTable.remove(opaque);
                    responseFuture.setCause(future.cause());
                    responseFuture.putResponse(null);
                    log.warn("send a request command to channel <" + addr + "> failed.");
                }
            });

            RemotingCommand responseCommand = responseFuture.waitResponse(timeoutMillis);
            if (null == responseCommand) {
                if (responseFuture.isSendRequestOK()) {
                    throw new RemotingTimeoutException(RemotingHelper.parseSocketAddressAddr(addr), timeoutMillis,
                            responseFuture.getCause());
                } else {
                    throw new RemotingSendRequestException(RemotingHelper.parseSocketAddressAddr(addr), responseFuture.getCause());
                }
            }
            log.info("invokeSyncImpl >> 返回数据 >> request = {}，responseCommand = {}", request, responseCommand);
            return responseCommand;
        } finally {
            this.responseTable.remove(opaque);
        }
    }

    public void putNettyEvent(final NettyEvent event) {
        this.nettyEventExecuter.putNettyEvent(event);
    }

    public abstract ChannelEventListener getChannelEventListener();

    class NettyEventExecuter extends ServiceThread {
        private final LinkedBlockingQueue<NettyEvent> eventQueue = new LinkedBlockingQueue<>();
        private final int maxSize = 10000;

        public void putNettyEvent(final NettyEvent event) {
            if (this.eventQueue.size() <= maxSize) {
                this.eventQueue.add(event);
            } else {
                log.warn("event queue size[{}] enough, so drop this event {}", this.eventQueue.size(), event.toString());
            }
        }

        @Override
        public String getServiceName() {
            return NettyEventExecuter.class.getSimpleName();
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            final ChannelEventListener listener = NettyRemotingAbstract.this.getChannelEventListener();
            while (!this.isStopped()) {
                try {
                    NettyEvent event = this.eventQueue.poll(3000, TimeUnit.MILLISECONDS);
                    if (event != null && listener != null) {
                        switch (event.getType()) {
                            case IDLE:
                                listener.onChannelIdle(event.getRemoteAddr(), event.getChannel());
                                break;
                            case CLOSE:
                                listener.onChannelClose(event.getRemoteAddr(), event.getChannel());
                                break;
                            case CONNECT:
                                listener.onChannelConnect(event.getRemoteAddr(), event.getChannel());
                                break;
                            case EXCEPTION:
                                listener.onChannelException(event.getRemoteAddr(), event.getChannel());
                                break;
                            default:
                                break;
                        }
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has com.rocketmq.client.exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }
    }
}