/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.remoting.protocol;

import com.alibaba.fastjson.JSON;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * @author xuleyan
 * @version RemotingSerializable.java, v 0.1 2020-10-11 12:01 下午
 */
public class RemotingSerializable {
    private static final Charset CHARSET_UTF8 = StandardCharsets.UTF_8;

    public static byte[] encode(final Object obj) {
        final String json = toJson(obj, false);
        if (json != null) {
            return json.getBytes(CHARSET_UTF8);
        }
        return null;
    }

    public static String toJson(Object obj, boolean prettyFormat) {
        return JSON.toJSONString(obj, prettyFormat);
    }

    public static <T> T decode(final byte[] data, Class<T> classOfT) {
        final String json = new String(data, CHARSET_UTF8);
        return fromJson(json, classOfT);
    }

    public static <T> T fromJson(String json, Class<T> classOfT) {
        return JSON.parseObject(json, classOfT);
    }

    public byte[] encode() {
        final String json = toJson(this, false);
        if (json != null) {
            return json.getBytes(CHARSET_UTF8);
        }
        return null;
    }

    public String toJson() {
        return toJson(false);
    }

    public String toJson(final boolean prettyFormat) {
        return toJson(this, prettyFormat);
    }

}