/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.broker.out;

import com.rocketmq.common.MixAll;
import com.rocketmq.common.namesrv.RegisterBrokerResult;
import com.rocketmq.common.namesrv.TopAddressing;
import com.rocketmq.common.protocol.RequestCode;
import com.rocketmq.common.protocol.ResponseCode;
import com.rocketmq.common.protocol.body.KVTable;
import com.rocketmq.common.protocol.body.RegisterBrokerBody;
import com.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import com.rocketmq.common.protocol.header.RegisterBrokerRequestHeader;
import com.rocketmq.common.protocol.header.namesrv.RegisterBrokerResponseHeader;
import com.rocketmq.remoting.RemotingCommand;
import com.rocketmq.remoting.RemotingClient;
import com.rocketmq.remoting.exception.RemotingCommandException;
import com.rocketmq.remoting.exception.RemotingConnectException;
import com.rocketmq.remoting.exception.RemotingSendRequestException;
import com.rocketmq.remoting.exception.RemotingTimeoutException;
import com.rocketmq.remoting.netty.NettyClientConfig;
import com.rocketmq.remoting.netty.NettyRemotingClient;
import com.rocketmq.client.exception.MQBrokerException;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @author xuleyan
 * @version BrokerOuterAPI.java, v 0.1 2020-11-01 9:10 下午
 */
@Slf4j
public class BrokerOuterAPI {
    private final RemotingClient remotingClient;
    private final TopAddressing topAddressing = new TopAddressing(MixAll.WS_ADDR);

    public BrokerOuterAPI(final NettyClientConfig nettyClientConfig) {
        this.remotingClient = new NettyRemotingClient(nettyClientConfig);
    }

    /**
     * 注册到多个 Namesrv
     *
     * @param clusterName        集群名
     * @param brokerAddr         broker地址
     * @param brokerName         brokerName
     * @param brokerId           brokerId
     * @param haServerAddr       高可用服务地址。用于broker master节点给 slave节点同步数据
     * @param topicConfigWrapper topic配置信息
     * @param filterServerList   filtersrv数组
     * @param oneway             是否oneway通信方式
     * @param timeoutMills       请求超时时间
     * @return 注册结果
     */
    public RegisterBrokerResult registerBrokerAll(
            final String clusterName,
            final String brokerAddr,
            final String brokerName,
            final long brokerId,
            final String haServerAddr,
            final TopicConfigSerializeWrapper topicConfigWrapper,
            final List<String> filterServerList,
            final boolean oneway,
            final int timeoutMills) throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException,
            RemotingConnectException, MQBrokerException, RemotingCommandException {

        RegisterBrokerResult result = new RegisterBrokerResult();
        List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
        if (nameServerAddressList != null) {
            for (String nameSrvAddr : nameServerAddressList) {
                RegisterBrokerRequestHeader requestHeader = new RegisterBrokerRequestHeader();
                requestHeader.setBrokerAddr(brokerAddr);
                requestHeader.setBrokerId(brokerId);
                requestHeader.setBrokerName(brokerName);
                requestHeader.setClusterName(clusterName);
                requestHeader.setHaServerAddr(haServerAddr);

                RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.REGISTER_BROKER, requestHeader);
                RegisterBrokerBody requestBody = new RegisterBrokerBody();
                requestBody.setTopicConfigSerializeWrapper(topicConfigWrapper);
                requestBody.setFilterServerList(filterServerList);
                request.setBody(requestBody.encode());

                RemotingCommand response = this.remotingClient.invokeSync(nameSrvAddr, request, timeoutMills);
                assert response != null;
                switch (response.getCode()) {
                    case ResponseCode.SUCCESS: {
                        RegisterBrokerResponseHeader responseHeader =
                                (RegisterBrokerResponseHeader) response.decodeCommandCustomHeader(RegisterBrokerResponseHeader.class);
                        result.setMasterAddr(Optional.ofNullable(responseHeader.getMasterAddr()).orElse(""));
                        result.setHaServerAddr(Optional.ofNullable(responseHeader.getHaServerAddr()).orElse(""));
                        if (response.getBody() != null) {
                            result.setKvTable(KVTable.decode(response.getBody(), KVTable.class));
                        }
                        return result;
                    }
                    default:
                        break;
                }

                throw new MQBrokerException(response.getCode(), response.getRemark());
            }
        }

        return result;
    }

    public void start() {
        if (remotingClient != null) {
            this.remotingClient.start();
        }
    }

    public void updateNameServerAddressList(String addrs) {
        List<String> list = new ArrayList<String>();
        String[] addrArray = addrs.split(";");
        for (String addr : addrArray) {
            list.add(addr);
        }
        this.remotingClient.updateNameServerAddressList(list);
    }
}