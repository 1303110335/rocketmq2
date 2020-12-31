/**
 * bianque.com
 * Copyright (C) 2013-2020All Rights Reserved.
 */
package com.rocketmq.remoting;

import com.rocketmq.remoting.exception.RemotingCommandException;

/**
 * @author xuleyan
 * @version CommandCustomHeader.java, v 0.1 2020-10-10 3:01 下午
 */
public interface CommandCustomHeader {

    void checkFields() throws RemotingCommandException;
}