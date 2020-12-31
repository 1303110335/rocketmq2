/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.remoting;

import lombok.extern.slf4j.Slf4j;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xuleyan
 * @version BrokerInfo.java, v 0.1 2020-11-29 12:16 下午
 */
@Slf4j
public class BrokerInfo {

    private final List<String> brokerAddrList = new ArrayList<>();

    public List<String> getBrokerAddrList() {
        return brokerAddrList;
    }

    /**
     * 注册broker列表地址
     *
     * @param brokerAddress
     */
    public void registerBrokerAddress(Object brokerAddress) {
        SocketAddress address = (SocketAddress) brokerAddress;
        brokerAddrList.add(address.toString() != null ? address.toString().substring(1) : address.toString());
        log.info("thread = {}, brokerAddrList = {}", Thread.currentThread().getName(), brokerAddrList);
    }
}