/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.common.protocol.header;

import com.rocketmq.remoting.CommandCustomHeader;
import com.rocketmq.remoting.RemotingCommand;
import com.rocketmq.remoting.exception.RemotingCommandException;
import com.rocketmq.remoting.protocol.RemotingCommandType;

import java.nio.charset.StandardCharsets;

/**
 * @author xuleyan
 * @version RegisterBrokerRequestHeader.java, v 0.1 2020-10-10 3:17 下午
 */
public class RegisterBrokerRequestHeader implements CommandCustomHeader {
    
    private String brokerName;
    
    private String brokerAddr;
    
    private String clusterName;
    
    private String haServerAddr;
    
    private Long brokerId;

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }

    public void setBrokerAddr(String brokerAddr) {
        this.brokerAddr = brokerAddr;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getHaServerAddr() {
        return haServerAddr;
    }

    public void setHaServerAddr(String haServerAddr) {
        this.haServerAddr = haServerAddr;
    }

    public Long getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(Long brokerId) {
        this.brokerId = brokerId;
    }

    public static void main(String[] args) {
        RemotingCommand remotingCommand = new RemotingCommand();
        remotingCommand.setType(RemotingCommandType.REQUEST_COMMAND);
        remotingCommand.setCode(103);
        remotingCommand.setOpaque(1);
        remotingCommand.setBody("哈哈".getBytes(StandardCharsets.UTF_8));

        CommandCustomHeader customHeader = new RegisterBrokerRequestHeader();
        ((RegisterBrokerRequestHeader)customHeader).setBrokerAddr("localhost:9876");
        ((RegisterBrokerRequestHeader)customHeader).setBrokerId(0L);
        ((RegisterBrokerRequestHeader)customHeader).setBrokerName("brokerA");
        remotingCommand.setCustomHeader(customHeader);

        System.out.println(remotingCommand);
    }

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    @Override
    public String toString() {
        return "RegisterBrokerRequestHeader{" +
                "brokerName='" + brokerName + '\'' +
                ", brokerAddr='" + brokerAddr + '\'' +
                ", clusterName='" + clusterName + '\'' +
                ", haServerAddr='" + haServerAddr + '\'' +
                ", brokerId=" + brokerId +
                '}';
    }
}