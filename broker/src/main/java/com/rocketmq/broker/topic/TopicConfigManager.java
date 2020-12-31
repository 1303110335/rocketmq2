/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.broker.topic;

import com.rocketmq.broker.BrokerController;
import com.rocketmq.broker.BrokerPathConfigHelper;
import com.rocketmq.common.ConfigManager;
import com.rocketmq.common.DataVersion;
import com.rocketmq.common.MixAll;
import com.rocketmq.common.constant.PermName;
import com.rocketmq.common.protocol.TopicConfig;
import com.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author xuleyan
 * @version TopicConfigManager.java, v 0.1 2020-12-10 9:00 下午
 */
@Slf4j
public class TopicConfigManager extends ConfigManager {

    private final Set<String> systemTopicList = new HashSet<String>();
    private final ConcurrentHashMap<String, TopicConfig> topicConfigTable =
            new ConcurrentHashMap<String, TopicConfig>(1024);

    private final DataVersion dataVersion = new DataVersion();
    private transient BrokerController brokerController;


    public TopicConfigManager() {
    }

    public TopicConfigManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        {
            // MixAll.SELF_TEST_TOPIC
            String topic = MixAll.SELF_TEST_TOPIC;
            TopicConfig topicConfig = new TopicConfig(topic);
            this.systemTopicList.add(topic);
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {
            // MixAll.DEFAULT_TOPIC
            // 当 producer 发送无Route路由信息 的消息时，使用MixAll.DEFAULT_TOPIC对应的路由进行发送
            // 发送到broker后，因为broker开启了自动创建Topic开关，则会创建该Topic路由，后面就可以使用该新路由进行发送消息
            if (this.brokerController.getBrokerConfig().isAutoCreateTopicEnable()) {
                String topic = MixAll.DEFAULT_TOPIC;
                TopicConfig topicConfig = new TopicConfig(topic);
                this.systemTopicList.add(topic);
                topicConfig.setReadQueueNums(this.brokerController.getBrokerConfig().getDefaultTopicQueueNums());
                topicConfig.setWriteQueueNums(this.brokerController.getBrokerConfig().getDefaultTopicQueueNums());
                int perm = PermName.PERM_INHERIT | PermName.PERM_READ | PermName.PERM_WRITE;
                topicConfig.setPerm(perm);
                this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
            }
        }
    }

    public TopicConfigSerializeWrapper buildTopicConfigSerializeWrapper() {
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        topicConfigSerializeWrapper.setTopicConfigTable(this.topicConfigTable);
        topicConfigSerializeWrapper.setDataVersion(this.dataVersion);
        return topicConfigSerializeWrapper;
    }

    @Override
    public String encode() {
        return encode(false);
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            TopicConfigSerializeWrapper topicConfigSerializeWrapper =
                    TopicConfigSerializeWrapper.fromJson(jsonString, TopicConfigSerializeWrapper.class);
            if (topicConfigSerializeWrapper != null) {
                this.topicConfigTable.putAll(topicConfigSerializeWrapper.getTopicConfigTable());
                this.dataVersion.assignNewOne(topicConfigSerializeWrapper.getDataVersion());
                this.printLoadDataWhenFirstBoot(topicConfigSerializeWrapper);
            }
        }
    }

    private void printLoadDataWhenFirstBoot(final TopicConfigSerializeWrapper tcs) {
        Iterator<Map.Entry<String, TopicConfig>> it = tcs.getTopicConfigTable().entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, TopicConfig> next = it.next();
            log.info("load exist local topic, {}", next.getValue().toString());
        }
    }

    @Override
    public String configFilePath() {
        return BrokerPathConfigHelper.getTopicConfigPath(this.brokerController.getMessageStoreConfig()
                .getStorePathRootDir());
    }

    @Override
    public String encode(boolean prettyFormat) {
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        topicConfigSerializeWrapper.setTopicConfigTable(this.topicConfigTable);
        topicConfigSerializeWrapper.setDataVersion(this.dataVersion);
        return topicConfigSerializeWrapper.toJson(prettyFormat);
    }
}