package com.rocketmq.broker.topic;


import com.rocketmq.broker.BrokerController;
import com.rocketmq.common.BrokerConfig;
import com.rocketmq.remoting.netty.NettyClientConfig;
import com.rocketmq.remoting.netty.NettyServerConfig;
import com.rocketmq.store.config.MessageStoreConfig;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
@Slf4j
public class TopicConfigManagerTest {

    @Spy
    private BrokerController brokerController = new BrokerController(new BrokerConfig(), new NettyServerConfig(), new NettyClientConfig(), new MessageStoreConfig());

    @Before
    public void init() {
        log.info("init");
    }

    @Test
    public void testLoadConfig() {
        TopicConfigManager topicConfigManager = new TopicConfigManager(brokerController);
        topicConfigManager.load();
    }

    @Test
    public void testSaveConfig() {
        TopicConfigManager topicConfigManager = new TopicConfigManager(brokerController);
        topicConfigManager.persist();
    }


}