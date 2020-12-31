/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.client.impl;

import com.rocketmq.client.ClientConfig;
import com.rocketmq.client.exception.MQBrokerException;
import com.rocketmq.client.exception.MQClientException;
import com.rocketmq.common.MixAll;
import com.rocketmq.common.namesrv.TopAddressing;
import com.rocketmq.common.protocol.RequestCode;
import com.rocketmq.common.protocol.ResponseCode;
import com.rocketmq.common.protocol.header.namesrv.GetRouteInfoRequestHeader;
import com.rocketmq.common.protocol.heartbeat.HeartbeatData;
import com.rocketmq.common.protocol.route.TopicRouteData;
import com.rocketmq.remoting.RPCHook;
import com.rocketmq.remoting.RemotingClient;
import com.rocketmq.remoting.RemotingCommand;
import com.rocketmq.remoting.exception.RemotingConnectException;
import com.rocketmq.remoting.exception.RemotingException;
import com.rocketmq.remoting.exception.RemotingSendRequestException;
import com.rocketmq.remoting.exception.RemotingTimeoutException;
import com.rocketmq.remoting.netty.NettyClientConfig;
import com.rocketmq.remoting.netty.NettyRemotingClient;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author xuleyan
 * @version MQClientAPIImpl.java, v 0.1 2020-12-15 9:03 下午
 */
@Slf4j
public class MQClientAPIImpl {

    /**
     * 远程调用Client
     */
    private final RemotingClient remotingClient;
    private final TopAddressing topAddressing;
    private final ClientRemotingProcessor clientRemotingProcessor;
    private String nameSrvAddr = null;
    private ClientConfig clientConfig;

    public MQClientAPIImpl(NettyClientConfig nettyClientConfig, ClientRemotingProcessor clientRemotingProcessor, RPCHook rpcHook, ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
        topAddressing = new TopAddressing(MixAll.WS_ADDR, clientConfig.getUnitName());
        this.remotingClient = new NettyRemotingClient(nettyClientConfig, null);
        this.clientRemotingProcessor = clientRemotingProcessor;

    }

    public void updateNameServerAddressList(String addrs) {
        List<String> lst = new ArrayList<String>();
        String[] addrArray = addrs.split(";");
        for (String addr : addrArray) {
            lst.add(addr);
        }

        this.remotingClient.updateNameServerAddressList(lst);
    }

    public void sendHearbeat(final String addr, HeartbeatData heartbeatData, final long timeoutMillis)
            throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
        request.setBody(heartbeatData.encode());
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);

        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS:{
                return;
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public void start() {
        this.remotingClient.start();
    }

    public void fetchNameServerAddr() {
        this.nameSrvAddr = "127.0.0.1:9876";
    }

    /**
     * 向 Namesrv 请求 Topic 路由信息
     *
     * @param topic         Topic
     * @param timeoutMillis 超时时间
     * @return
     * @throws RemotingException    调用异常
     * @throws MQClientException    调用返回非SUCCESS
     * @throws InterruptedException 中断
     */
    public TopicRouteData getTopicRouteInfoFromNameServer(final String topic, final long timeoutMillis) throws InterruptedException, RemotingException, MQClientException {
        GetRouteInfoRequestHeader requestHeader = new GetRouteInfoRequestHeader();
        requestHeader.setTopic(topic);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINTO_BY_TOPIC, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;

        switch (response.getCode()) {
            case ResponseCode.TOPIC_NOT_EXIST:
                if (!topic.equals(MixAll.DEFAULT_TOPIC)) {
                    log.warn("get Topic [{}] RouteInfoFromNameServer is not exist value", topic);
                }
                break;
            case ResponseCode.SUCCESS:
                byte[] body = response.getBody();
                if (body != null) {
                    return TopicRouteData.decode(body, TopicRouteData.class);
                }
            default:
                break;
        }
        throw new MQClientException(response.getCode(), response.getRemark());
    }
}