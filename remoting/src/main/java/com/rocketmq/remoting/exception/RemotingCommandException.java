/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.remoting.exception;

/**
 * @author xuleyan
 * @version RemotingCommandException.java, v 0.1 2020-10-11 11:32 上午
 */
public class RemotingCommandException extends RemotingException {

    private static final long serialVersionUID = 5165440252787472214L;

    public RemotingCommandException(String message) {
        super(message, null);
    }

    public RemotingCommandException(String message, Throwable cause) {
        super(message, cause);
    }

}