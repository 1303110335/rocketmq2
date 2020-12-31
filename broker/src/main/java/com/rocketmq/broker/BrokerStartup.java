/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.broker;

import com.rocketmq.common.BrokerConfig;
import com.rocketmq.common.MixAll;
import com.rocketmq.remoting.common.RemotingUtil;
import com.rocketmq.remoting.netty.NettyClientConfig;
import com.rocketmq.remoting.netty.NettyServerConfig;
import com.rocketmq.store.config.BrokerRole;
import com.rocketmq.store.config.MessageStoreConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.Properties;

/**
 * @author xuleyan
 * @version BrokerStartup.java, v 0.1 2020-10-20 11:21 下午
 */
@Slf4j
public class BrokerStartup {
    public static String configFile = null;
    public static Properties properties = null;

    public static void main(String[] args) {
        start(createBrokerController(args));
    }

    public static BrokerController start(BrokerController controller) {
        try {
            controller.start();
            String tip = "The broker[" + controller.getBrokerConfig().getBrokerName() + ", "
                    + controller.getBrokerAddr() + "] boot success.";

            if (null != controller.getBrokerConfig().getNamesrvAddr()) {
                tip += " and name server is " + controller.getBrokerConfig().getNamesrvAddr();
            }

            log.info(tip);
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return null;
    }

    private static BrokerController createBrokerController(String[] args) {

        final BrokerConfig brokerConfig = new BrokerConfig();
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        final NettyClientConfig nettyClientConfig = new NettyClientConfig();
        nettyServerConfig.setListenPort(10911);
        nettyServerConfig.setServerSelectorThreads(3);

        brokerConfig.setBrokerClusterName("clusterA");
        brokerConfig.setBrokerIP1("localhost");
        brokerConfig.setBrokerName("brokerA");

        brokerConfig.setNamesrvAddr("localhost:9876");
        brokerConfig.setRegisterBrokerTimeoutMills(100000);
        brokerConfig.setDefaultTopicQueueNums(4);

        final MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        if (BrokerRole.SLAVE == messageStoreConfig.getBrokerRole()) {
            int ratio = messageStoreConfig.getAccessMessageInMemoryMaxRatio() - 10;
            messageStoreConfig.setAccessMessageInMemoryMaxRatio(ratio);
        }

        switch (messageStoreConfig.getBrokerRole()) {
            case ASYNC_MASTER:
            case SYNC_MASTER:
                brokerConfig.setBrokerId(MixAll.MASTER_ID);
                break;
            case SLAVE:
                if (brokerConfig.getBrokerId() <= 0) {
                    System.out.printf("Slave's brokerId must be > 0");
                    System.exit(-3);
                }

                break;
            default:
                break;
        }
        messageStoreConfig.setHaListenPort(nettyServerConfig.getListenPort() + 1);

        String namesrvAddr = brokerConfig.getNamesrvAddr();
        if (namesrvAddr != null) {
            try {
                String[] addrArray = namesrvAddr.split(";");
                for (String addr : addrArray) {
                    RemotingUtil.string2SocketAddress(addr);
                }
            } catch (Exception e) {
                System.out.printf(
                        "The Name Server Address[%s] illegal, please set it as follows, \"127.0.0.1:9876;192.168.0.1:9876\"%n",
                        namesrvAddr);
                System.exit(-3);
            }
        }

        final BrokerController controller = new BrokerController(brokerConfig, nettyServerConfig, nettyClientConfig, messageStoreConfig);
        //controller.getConfiguration().registerConfig(properties);

        boolean initResult = controller.initialize();
        if (!initResult) {
            controller.shutdown();
            System.exit(-3);
        }

        return controller;

    }
}