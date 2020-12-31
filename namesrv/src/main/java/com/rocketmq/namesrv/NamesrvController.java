package com.rocketmq.namesrv; /**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */


import com.rocketmq.common.ThreadFactoryImpl;
import com.rocketmq.namesrv.prcessor.DefaultRequestProcessor;
import com.rocketmq.namesrv.routeinfo.BrokerHousekeepingService;
import com.rocketmq.namesrv.routeinfo.RouteInfoManager;
import com.rocketmq.remoting.RemotingServer;
import com.rocketmq.remoting.netty.NettyRemotingServer;
import com.rocketmq.remoting.netty.NettyServerConfig;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author xuleyan
 * @version com.rocketmq.namesrv.NamesrvController.java, v 0.1 2020-11-29 12:19 下午
 */
public class NamesrvController {


    private final RouteInfoManager routeInfoManager;

    private RemotingServer remotingServer;

    private ExecutorService remotingService;

    private final NettyServerConfig nettyServerConfig;

    private BrokerHousekeepingService brokerHousekeepingService;

    public RemotingServer getRemotingServer() {
        return remotingServer;
    }

    public void setRemotingServer(RemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }


    public RouteInfoManager getRouteInfoManager() {
        return routeInfoManager;
    }

    public NamesrvController(NettyServerConfig nettyServerConfig) {
        this.nettyServerConfig = nettyServerConfig;
        this.routeInfoManager = new RouteInfoManager();
        this.brokerHousekeepingService = new BrokerHousekeepingService(this);
    }

    public void init() {
        this.remotingServer = new NettyRemotingServer(nettyServerConfig, this.brokerHousekeepingService);

        remotingService = Executors.newFixedThreadPool(nettyServerConfig.getServerWorkerThreads(), new ThreadFactoryImpl("RemotingExecutorThread_"));
        remotingServer.registerDefaultProcessor(new DefaultRequestProcessor(this), remotingService);
    }

    public void start() {
        remotingServer.start();
    }
}