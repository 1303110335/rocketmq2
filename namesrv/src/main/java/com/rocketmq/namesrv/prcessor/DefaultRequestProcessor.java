/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.namesrv.prcessor;

import com.alibaba.fastjson.JSON;
import com.rocketmq.common.help.FAQUrl;
import com.rocketmq.common.namesrv.RegisterBrokerResult;
import com.rocketmq.common.protocol.RequestCode;
import com.rocketmq.common.protocol.ResponseCode;
import com.rocketmq.common.protocol.body.RegisterBrokerBody;
import com.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import com.rocketmq.common.protocol.header.RegisterBrokerRequestHeader;
import com.rocketmq.common.protocol.header.namesrv.GetRouteInfoRequestHeader;
import com.rocketmq.common.protocol.header.namesrv.RegisterBrokerResponseHeader;
import com.rocketmq.common.protocol.route.TopicRouteData;
import com.rocketmq.namesrv.NamesrvController;
import com.rocketmq.remoting.CommandCustomHeader;
import com.rocketmq.remoting.RemotingCommand;
import com.rocketmq.remoting.common.RemotingHelper;
import com.rocketmq.remoting.exception.RemotingCommandException;
import com.rocketmq.remoting.netty.NettyRequestProcessor;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author xuleyan
 * @version DefaultRequestProcessor.java, v 0.1 2020-12-03 3:32 下午
 */
@Slf4j
public class DefaultRequestProcessor implements NettyRequestProcessor {
    protected final NamesrvController namesrvController;

    public DefaultRequestProcessor(NamesrvController namesrvController) {
        this.namesrvController = namesrvController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        log.debug("DefaultRequestProcessor >> {} receive request, ctx = {}, remoteAddr = {}, request = {}",
                Thread.currentThread().getName(), ctx, RemotingHelper.parseChannelRemoteAddr(ctx.channel()), request);

        switch (request.getCode()) {
            case RequestCode.REGISTER_BROKER:
                return this.registerBroker(ctx, request);
            case RequestCode.GET_ROUTEINTO_BY_TOPIC:
                return this.getRouteInfoByTopic(ctx, request);
            default:
                break;
        }
        return null;
    }

    /**
     * 根据topic查询路由信息
     * @param ctx
     * @param request
     * @return
     */
    private RemotingCommand getRouteInfoByTopic(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        GetRouteInfoRequestHeader requestHeader = (GetRouteInfoRequestHeader)request.decodeCommandCustomHeader(GetRouteInfoRequestHeader.class);
        TopicRouteData topicRouteData = this.namesrvController.getRouteInfoManager().pickupTopicRouteData(requestHeader.getTopic());
        if (topicRouteData != null) {
            byte[] content = topicRouteData.encode();
            response.setBody(content);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }

        response.setCode(ResponseCode.TOPIC_NOT_EXIST);
        response.setRemark("No topic route info in name server for the topic: " + requestHeader.getTopic()
                + FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL));
        return response;
    }

    private RemotingCommand registerBroker(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(RegisterBrokerResponseHeader.class);
        final RegisterBrokerResponseHeader responseHeader = (RegisterBrokerResponseHeader) response.readCustomHeader();

        RegisterBrokerRequestHeader requestHeader = (RegisterBrokerRequestHeader) request.decodeCommandCustomHeader(RegisterBrokerRequestHeader.class);

        TopicConfigSerializeWrapper topicConfigWrapper;
        RegisterBrokerBody registerBrokerBody = new RegisterBrokerBody();
        if (request.getBody() != null) {
            registerBrokerBody = RegisterBrokerBody.decode(request.getBody(), RegisterBrokerBody.class);
        } else {
            topicConfigWrapper = new TopicConfigSerializeWrapper();
            topicConfigWrapper.getDataVersion().setCounter(new AtomicLong(0));
            topicConfigWrapper.getDataVersion().setTimestamp(0);
        }

        RegisterBrokerResult result = this.namesrvController.getRouteInfoManager().registerBroker(
                requestHeader.getClusterName(),
                requestHeader.getBrokerAddr(),
                requestHeader.getBrokerName(),
                requestHeader.getBrokerId(),
                requestHeader.getHaServerAddr(),
                registerBrokerBody.getTopicConfigSerializeWrapper(),
                null,
                ctx.channel()
        );

        responseHeader.setHaServerAddr(result.getHaServerAddr());
        responseHeader.setMasterAddr(result.getMasterAddr());
        response.setBody(new byte[]{});
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

}