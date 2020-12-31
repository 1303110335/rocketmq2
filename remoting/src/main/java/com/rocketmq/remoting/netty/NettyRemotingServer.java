/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.remoting.netty;

import com.rocketmq.remoting.ChannelEventListener;
import com.rocketmq.remoting.InvokeCallback;
import com.rocketmq.remoting.RemotingCommand;
import com.rocketmq.remoting.common.Pair;
import com.rocketmq.remoting.common.RemotingHelper;
import com.rocketmq.remoting.common.RemotingUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.extern.slf4j.Slf4j;

import java.nio.channels.Channel;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author xuleyan
 * @version SomeServer.java, v 0.1 2020-09-30 5:10 下午
 */
@Slf4j
public class NettyRemotingServer extends NettyRemotingAbstract {

    private final ServerBootstrap bootstrap;
    private final NettyServerConfig nettyServerConfig;

    private final EventLoopGroup parentGroup;
    private final EventLoopGroup childGroup;

    private final ChannelEventListener channelEventListener;
    private final ExecutorService publicExecutor;


    public NettyRemotingServer(final NettyServerConfig nettyServerConfig, ChannelEventListener channelEventListener) {
        this.nettyServerConfig = nettyServerConfig;
        this.channelEventListener = channelEventListener;

        int publicThreadNums = nettyServerConfig.getServerCallbackExecutorThreads();
        if (publicThreadNums <= 0) {
            publicThreadNums = 4;
        }

        this.publicExecutor = Executors.newFixedThreadPool(publicThreadNums, new ThreadFactory() {

            private AtomicInteger threadIndex = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "NettyServerPublicExecutor_" + threadIndex.incrementAndGet());
            }
        });

        bootstrap = new ServerBootstrap();
        parentGroup = new NioEventLoopGroup(1, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("parentGroup_%d", this.threadIndex.incrementAndGet()));
            }
        });

        childGroup = new NioEventLoopGroup(nettyServerConfig.getServerSelectorThreads(), new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("childGroup_%d", this.threadIndex.incrementAndGet()));
            }
        });

    }

    @Override
    public void start() {
    
        try {
            bootstrap.group(parentGroup, childGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {

                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            // 获取channel中的Pipeline
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(
                                    new NettyEncoder(),
                                    new NettyDecoder(),
                                    new IdleStateHandler(0, 0, nettyServerConfig.getServerChannelMaxIdleTimeSeconds()),
                                    new NettyConnectManageHandler(),
                                    new NamesrvServerHandler());
                        }
                    });
            ChannelFuture future = bootstrap.bind(nettyServerConfig.getListenPort()).sync();
            log.info("{} start >> 服务器已启动, 端口:{}", Thread.currentThread().getName(), nettyServerConfig.getListenPort());
            //future.channel().closeFuture().sync();

            //启动nettyEvent事件处理线程池
            if (this.channelEventListener != null) {
                this.nettyEventExecuter.start();
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void shutdown() {
        if (parentGroup != null) {
            parentGroup.shutdownGracefully();
        }
        if (childGroup != null) {
            childGroup.shutdownGracefully();
        }
    }


    @Override
    public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor) {
        ExecutorService executorThis = executor;
        if (null == executor) {
            executorThis = this.publicExecutor;
        }

        Pair<NettyRequestProcessor, ExecutorService> pair = new Pair<>(processor, executorThis);
        this.processTable.put(requestCode, pair);
    }

    @Override
    public RemotingCommand invokeSync(Channel channel, RemotingCommand request, long timeoutMillis) {
        return null;
    }

    @Override
    public void invokeAsync(Channel channel, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback) {

    }

    @Override
    public void invokeOneway(Channel channel, RemotingCommand request, long timeoutMillis) {

    }

    @Override
    public ChannelEventListener getChannelEventListener() {
        return this.channelEventListener;
    }

    class NettyConnectManageHandler extends ChannelDuplexHandler {
        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelRegistered {}", remoteAddress);
            super.channelRegistered(ctx);
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelUnregistered, the channel[{}]", remoteAddress);
            super.channelUnregistered(ctx);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelActive, the channel[{}]", remoteAddress);
            super.channelActive(ctx);

            if (NettyRemotingServer.this.channelEventListener != null) {
                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.CONNECT, remoteAddress, ctx.channel()));
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelInactive, the channel[{}]", remoteAddress);
            super.channelInactive(ctx);

            if (NettyRemotingServer.this.channelEventListener != null) {
                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state().equals(IdleState.ALL_IDLE)) {
                    final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    log.warn("NETTY SERVER PIPELINE: IDLE com.rocketmq.client.exception [{}]", remoteAddress);
                    RemotingUtil.closeChannel(ctx.channel());
                    if (NettyRemotingServer.this.channelEventListener != null) {
                        NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.IDLE, remoteAddress, ctx.channel()));
                    }
                }
            }
        }
    }


    public class NamesrvServerHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            processMessageReceived(ctx, (RemotingCommand)msg);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            ctx.close();
        }
    }
}