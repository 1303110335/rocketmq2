/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.broker.processor;

import com.rocketmq.broker.BrokerController;
import com.rocketmq.broker.client.ClientChannelInfo;
import com.rocketmq.common.protocol.RequestCode;
import com.rocketmq.common.protocol.ResponseCode;
import com.rocketmq.common.protocol.heartbeat.HeartbeatData;
import com.rocketmq.common.protocol.heartbeat.ProducerData;
import com.rocketmq.remoting.RemotingCommand;
import com.rocketmq.remoting.exception.RemotingCommandException;
import com.rocketmq.remoting.netty.NettyRequestProcessor;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author xuleyan
 * @version ClientManageProcessor.java, v 0.1 2020-12-15 9:45 下午
 */
@Slf4j
public class ClientManageProcessor implements NettyRequestProcessor {

    private final BrokerController brokerController;

    public ClientManageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        log.info("ClientManageProcessor >> processRequest >> request = {}", request);
        switch (request.getCode()) {
            case RequestCode.HEART_BEAT:
                return this.heartBeat(ctx, request);
            default:
                break;
        }
        return null;
    }

    /**
     * 消费者或生产者的注册
     * @param ctx
     * @param request
     * @return
     */
    private RemotingCommand heartBeat(ChannelHandlerContext ctx, RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        HeartbeatData heartbeatData = HeartbeatData.decode(request.getBody(), HeartbeatData.class);
        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
                ctx.channel(), heartbeatData.getClientID(), request.getLanguage(), request.getVersion());

        // 消费者注册

        // 生产者注册
        for (ProducerData data : heartbeatData.getProducerDataSet()) {
            this.brokerController.getProducerManager().registerProducer(data.getGroupName(), clientChannelInfo);

        }
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }
}