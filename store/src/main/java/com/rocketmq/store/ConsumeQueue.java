/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.store;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author xuleyan
 * @version ConsumeQueue.java, v 0.1 2020-12-10 9:19 下午
 */
@Slf4j
public class ConsumeQueue {

    public static final int CQ_STORE_UNIT_SIZE = 20;  //消费队列存储单元大小,即每条消息相关信息大小，20字节,8个字节的偏移量,4个字节的消息长度,8个字节的Tag HashCode

}