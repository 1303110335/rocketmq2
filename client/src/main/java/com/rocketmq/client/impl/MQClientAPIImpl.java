/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.client.impl;

import com.rocketmq.client.ClientConfig;
import com.rocketmq.client.exception.MQBrokerException;
import com.rocketmq.client.exception.MQClientException;
import com.rocketmq.client.hook.SendMessageContext;
import com.rocketmq.client.impl.factory.MQClientInstance;
import com.rocketmq.client.impl.producer.TopicPublishInfo;
import com.rocketmq.client.producer.SendCallback;
import com.rocketmq.client.producer.SendResult;
import com.rocketmq.common.MixAll;
import com.rocketmq.common.message.Message;
import com.rocketmq.common.namesrv.TopAddressing;
import com.rocketmq.common.protocol.RequestCode;
import com.rocketmq.common.protocol.ResponseCode;
import com.rocketmq.common.protocol.header.SendMessageRequestHeader;
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
import java.util.concurrent.atomic.AtomicInteger;

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

    public TopicRouteData getDefaultTopicRouteInfoFromNameServer(final String topic, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        GetRouteInfoRequestHeader requestHeader = new GetRouteInfoRequestHeader();
        requestHeader.setTopic(topic);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINTO_BY_TOPIC, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.TOPIC_NOT_EXIST: {
                // TODO LOG
                break;
            }
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    return TopicRouteData.decode(body, TopicRouteData.class);
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    /**
     * 发送消息，并返回发送结果
     *
     * @param addr              broker地址
     * @param brokerName        brokerName
     * @param msg               消息
     * @param requestHeader     请求
     * @param timeoutMillis     请求最大时间
     * @param communicationMode 通信模式
     * @param context           发送消息context
     * @param producer          producer
     * @return 发送结果
     * @throws RemotingException 当请求发生异常
     * @throws MQBrokerException 当Broker发生异常
     * @throws InterruptedException 当线程被打断
     */
    public SendResult sendMessage(//
                                  final String addr, // 1
                                  final String brokerName, // 2
                                  final Message msg, // 3
                                  final SendMessageRequestHeader requestHeader, // 4
                                  final long timeoutMillis, // 5
                                  final CommunicationMode communicationMode, // 6
                                  final SendMessageContext context, // 7
                                  final DefaultMQProducerImpl producer // 8
    ) throws RemotingException, MQBrokerException, InterruptedException {
        return sendMessage(addr, brokerName, msg, requestHeader, timeoutMillis, communicationMode, null, null, null, 0, context, producer);
    }

    /**
     * 发送消息，并返回发送结果
     *
     * @param addr                     broker地址
     * @param brokerName               brokerName
     * @param msg                      消息
     * @param requestHeader            请求
     * @param timeoutMillis            请求最大时间
     * @param communicationMode        通信模式
     * @param sendCallback             发送回调
     * @param topicPublishInfo         topic发布信息
     * @param instance                 client
     * @param retryTimesWhenSendFailed
     * @param context                  发送消息context
     * @param producer                 producer
     * @return 发送结果
     * @throws RemotingException 当请求发生异常
     * @throws MQBrokerException 当Broker发生异常
     * @throws InterruptedException 当线程被打断
     */
    public SendResult sendMessage(//
                                  final String addr, // 1
                                  final String brokerName, // 2
                                  final Message msg, // 3
                                  final SendMessageRequestHeader requestHeader, // 4
                                  final long timeoutMillis, // 5
                                  final CommunicationMode communicationMode, // 6
                                  final SendCallback sendCallback, // 7
                                  final TopicPublishInfo topicPublishInfo, // 8
                                  final MQClientInstance instance, // 9
                                  final int retryTimesWhenSendFailed, // 10
                                  final SendMessageContext context, // 11
                                  final DefaultMQProducerImpl producer // 12
    ) throws RemotingException, MQBrokerException, InterruptedException {

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, requestHeader);
        request.setBody(msg.getBody());

        switch (communicationMode) {
            case ONEWAY:
                this.remotingClient.invokeOneway(addr, request, timeoutMillis);
                return null;
            case ASYNC:
                final AtomicInteger times = new AtomicInteger();
                this.sendMessageAsync(addr, brokerName, msg, timeoutMillis, request, sendCallback, topicPublishInfo, instance,
                        retryTimesWhenSendFailed, times, context, producer);
                return null;
            case SYNC:
                return this.sendMessageSync(addr, brokerName, msg, timeoutMillis, request);
            default:
                assert false;
                break;
        }
        return null;
    }

    /**
     * 发布同步消息，并返回发送结果
     *
     * @param addr          broker地址
     * @param brokerName    brokerName
     * @param msg           消息
     * @param timeoutMillis 请求最大时间
     * @param request       请求
     * @return 发送结果
     * @throws RemotingException 当请求发生异常
     * @throws MQBrokerException 当Broker发生异常
     * @throws InterruptedException 当线程被打断
     */
    private SendResult sendMessageSync(//
                                       final String addr, //
                                       final String brokerName, //
                                       final Message msg, //
                                       final long timeoutMillis, //
                                       final RemotingCommand request//
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        return this.processSendResponse(brokerName, msg, response);
    }

    private SendResult processSendResponse(String brokerName, Message msg, RemotingCommand response) {
        
        return null;
    }

    private void sendMessageAsync(String addr, String brokerName, Message msg, long timeoutMillis, RemotingCommand request, SendCallback sendCallback, TopicPublishInfo topicPublishInfo, MQClientInstance instance, int retryTimesWhenSendFailed, AtomicInteger times, SendMessageContext context, DefaultMQProducerImpl producer) {
    }
}