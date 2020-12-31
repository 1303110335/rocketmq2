/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.remoting.netty;

import com.rocketmq.remoting.ChannelEventListener;
import com.rocketmq.remoting.InvokeCallback;
import com.rocketmq.remoting.RemotingCommand;
import com.rocketmq.remoting.RemotingClient;
import com.rocketmq.remoting.common.RemotingHelper;
import com.rocketmq.remoting.common.RemotingUtil;
import com.rocketmq.remoting.exception.RemotingConnectException;
import com.rocketmq.remoting.exception.RemotingSendRequestException;
import com.rocketmq.remoting.exception.RemotingTimeoutException;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * @author xuleyan
 * @version NettyRemotingClient.java, v 0.1 2020-12-06 10:49 下午
 */
@Slf4j
public class NettyRemotingClient extends NettyRemotingAbstract implements RemotingClient {

    private final AtomicReference<List<String>> namesrvAddrList = new AtomicReference<>();
    private final Bootstrap bootstrap = new Bootstrap();
    private final EventLoopGroup eventLoopGroupWorker;
    private final NettyClientConfig nettyClientConfig;
    private final ChannelEventListener channelEventListener;
    private final AtomicReference<String> namesrvAddrChoosed = new AtomicReference<>();
    private final Lock lockNamesrvChannel = new ReentrantLock();
    private final Lock lockChannelTable = new ReentrantLock();
    private final AtomicInteger namesrvIndex = new AtomicInteger(initValueIndex());

    private static int initValueIndex() {
        Random r = new Random();
        return Math.abs(r.nextInt() % 999) % 999;
    }

    private static final long LOCK_TIMEOUT_MILLIS = 3000;

    private final ConcurrentHashMap<String, ChannelWrapper> channelTables = new ConcurrentHashMap<>();

    public NettyRemotingClient(final NettyClientConfig nettyClientConfig) {
        this(nettyClientConfig, null);
    }

    public NettyRemotingClient(NettyClientConfig nettyClientConfig, final ChannelEventListener channelEventListener) {
        this.nettyClientConfig = nettyClientConfig;
        this.channelEventListener = channelEventListener;

        this.eventLoopGroupWorker = new NioEventLoopGroup(1, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("NettyClientSelector_%d", this.threadIndex.incrementAndGet()));
            }
        });
    }

    @Override
    public RemotingCommand invokeSync(String addr, RemotingCommand request, long timeoutMillis) throws InterruptedException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException {

        final Channel channel = this.getAndCreateChannel(addr);
        if (channel != null && channel.isActive()) {
            try {
                RemotingCommand response = this.invokeSyncImpl(channel, request, timeoutMillis);
                return response;
            } catch (RemotingSendRequestException e) {
                log.warn("invokeSync: send request com.rocketmq.client.exception, so close the channel[{}]", addr);
                this.closeChannel(channel);
                throw e;
            } catch (RemotingTimeoutException e) {
                if (nettyClientConfig.isClientCloseSocketIfTimeout()) {
                    this.closeChannel(addr, channel);
                    log.warn("invokeSync: close socket because of timeout, {}ms, {}", timeoutMillis, addr);
                }
                log.warn("invokeSync: wait response timeout com.rocketmq.client.exception, the channel[{}]", addr);
                throw e;
            }
        } else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }

    private void closeChannel(Channel channel) {
        RemotingUtil.closeChannel(channel);
    }

    private void closeChannel(String addr, Channel channel) {
        channelTables.remove(addr);
        RemotingUtil.closeChannel(channel);
    }


    private Channel getAndCreateChannel(final String addr) throws InterruptedException {
        if (null == addr) {
            return getAndCreateNameserverChannel();
        }
        //当addr不为空时,先从缓存中获取
        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {
            return cw.getChannel();
        }

        return this.createChannel(addr);
    }

    /**
     * 获取NamesrvAddr
     * {@link #namesrvAddrList}里存储的值是客户端初始化时指定的
     * 多个NamesrcAddr间用 ";" 隔开
     *
     * @return
     * @throws InterruptedException
     */
    private Channel getAndCreateNameserverChannel() throws InterruptedException {
        String addr = this.namesrvAddrChoosed.get();
        if (addr != null) {
            ChannelWrapper cw = this.channelTables.get(addr);
            if (cw != null && cw.isOK()) {
                return cw.getChannel();
            }
        }

        final List<String> addrList = this.namesrvAddrList.get();
        if (this.lockNamesrvChannel.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
            try {
                addr = this.namesrvAddrChoosed.get();
                if (addr != null) {
                    ChannelWrapper cw = this.channelTables.get(addr);
                    if (cw != null && cw.isOK()) {
                        return cw.getChannel();
                    }
                }
                // 从【Namesrv列表】随机选择一个,创建Channel成功了就返回;
                // 不成功就从上个开始按顺序依次尝试,直到有成功的为止
                if (addrList != null && addrList.size() > 0) {
                    for (int i = 0; i < addrList.size(); i++) {
                        int index = this.namesrvIndex.incrementAndGet();
                        index = Math.abs(index);
                        index = index % addrList.size();
                        String newAddr = addrList.get(index);
                        this.namesrvAddrChoosed.set(newAddr);
                        Channel channelNew = this.createChannel(newAddr);
                        if (channelNew != null) {
                            return channelNew;
                        }
                    }
                }
            } catch (Exception e) {
                log.error("getAndCreateNameserverChannel: create name server channel exception", e);
            } finally {
                this.lockNamesrvChannel.unlock();
            }
        } else {
            log.warn("getAndCreateNameserverChannel: try to lock name server, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
        }

        return null;
    }

    /**
     * 根据指定的addr创建Channel,创建成功了放入{@link #channelTables}中以备下次复用
     *
     * @param addr
     * @return
     * @throws InterruptedException
     */
    private Channel createChannel(String addr) throws InterruptedException {
        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {
            return cw.getChannel();
        }

        if (this.lockChannelTable.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
            try {
                boolean createNewConnection = false;
                cw = this.channelTables.get(addr);
                if (cw != null) {
                    if (cw.isOK()) {
                        return cw.getChannel();
                    } else if (!cw.getChannelFuture().isDone()) {
                        createNewConnection = false;
                    } else {
                        this.channelTables.remove(addr);
                        createNewConnection = true;
                    }
                } else {
                    createNewConnection = true;
                }

                if (createNewConnection) {
                    ChannelFuture channelFuture = this.bootstrap.connect(RemotingHelper.string2SocketAddress(addr));
                    log.info("createChannel: begin to connect remote host[{}] asynchronously", addr);
                    cw = new ChannelWrapper(channelFuture);
                    this.channelTables.put(addr, cw);
                }
            } catch (Exception e) {
                log.error("createChannel: create channel exception", e);
            } finally {
                this.lockChannelTable.unlock();
            }
        } else {
            log.warn("createChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
        }

        if (cw != null) {
            ChannelFuture channelFuture = cw.getChannelFuture();
            if (channelFuture.awaitUninterruptibly(this.nettyClientConfig.getConnectTimeoutMillis())) {
                if (cw.isOK()) {
                    log.info("createChannel: connect remote host[{}] success, {}", addr, channelFuture.toString());
                    return cw.getChannel();
                } else {
                    log.warn("createChannel: connect remote host[" + addr + "] failed, " + channelFuture.toString(), channelFuture.cause());
                }
            } else {
                log.warn("createChannel: connect remote host[{}] timeout {}ms, {}", addr, this.nettyClientConfig.getConnectTimeoutMillis(),
                        channelFuture.toString());
            }
        }
        return null;
    }

    @Override
    public void invokeAsync(String addr, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback) throws InterruptedException {

    }

    @Override
    public void invokeOneway(String addr, RemotingCommand request, long timeoutMillis) throws InterruptedException {

    }

    @Override
    public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor) {

    }

    @Override
    public void updateNameServerAddressList(List<String> addrs) {
        List<String> old = this.namesrvAddrList.get();

        boolean update = false;
        if (!addrs.isEmpty()) {
            if (null == old) {
                update = true;
            } else if (addrs.size() != old.size()) {
                update = true;
            } else {
                for (String addr : addrs) {
                    if (!old.contains(addr)) {
                        update = true;
                    }
                }
            }

            if (update) {
                this.namesrvAddrList.set(addrs);
            }
        }
    }

    @Override
    public List<String> getNameServerAddressList() {
        return this.namesrvAddrList.get();
    }

    @Override
    public void start() {
        bootstrap.group(eventLoopGroupWorker)
                .channel(NioSocketChannel.class)
                // Nagle算法通过减少需要传输的数据包，来优化网络
                // 启动TCP_NODELAY，就意味着禁用了Nagle算法，允许小包的发送
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {

                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new NettyEncoder());
                        pipeline.addLast(new NettyDecoder());
                        pipeline.addLast(new NettyClientHandler());
                    }
                });

    }

    @Override
    public void shutdown() {

    }

    @Override
    public RemotingCommand invokeSync(java.nio.channels.Channel channel, RemotingCommand request, long timeoutMillis) {
        return null;
    }

    @Override
    public void invokeAsync(java.nio.channels.Channel channel, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback) {

    }

    @Override
    public void invokeOneway(java.nio.channels.Channel channel, RemotingCommand request, long timeoutMillis) {

    }

    @Override
    public ChannelEventListener getChannelEventListener() {
        return channelEventListener;
    }

    private class NettyClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            processMessageReceived(ctx, msg);
        }
    }

    static class ChannelWrapper {
        private final ChannelFuture channelFuture;

        ChannelWrapper(ChannelFuture channelFuture) {
            this.channelFuture = channelFuture;
        }

        public boolean isOK() {
            return this.channelFuture.channel() != null && this.channelFuture.channel().isActive();
        }

        public boolean isWriteable() {
            return this.channelFuture.channel().isWritable();
        }

        private Channel getChannel() {
            return this.channelFuture.channel();
        }

        public ChannelFuture getChannelFuture() {
            return channelFuture;
        }
    }
}