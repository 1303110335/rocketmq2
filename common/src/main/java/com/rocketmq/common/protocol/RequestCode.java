/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.common.protocol;

/**
 * @author xuleyan
 * @version RequestCode.java, v 0.1 2020-10-10 2:36 下午
 */
public class RequestCode {
    /**
     * 注册 Broker
     * Broker => Namesrv
     * 时间：
     * - Broker 初始化启动时
     * - Broker 每30秒注册次
     */
    public static final int REGISTER_BROKER = 103;

    public static final int HEART_BEAT = 34;

    public static final int UNREGISTER_CLIENT = 35;

    public static final int GET_ROUTEINTO_BY_TOPIC = 105;
}