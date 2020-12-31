/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.common.namesrv;

import com.rocketmq.common.MixAll;
import com.rocketmq.common.UtilAll;
import com.rocketmq.common.help.FAQUrl;
import com.rocketmq.common.utils.HttpTinyClient;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 * @author xuleyan
 * @version TopAddressing.java, v 0.1 2020-11-01 9:13 下午
 */
@Slf4j
public class TopAddressing {
    private String nsAddr;
    private String wsAddr;
    private String unitName;

    public TopAddressing(final String wsAddr) {
        this(wsAddr, null);
    }

    public TopAddressing(final String wsAddr, final String unitName) {
        this.wsAddr = wsAddr;
        this.unitName = unitName;
    }

    private static String clearNewLine(final String str) {
        String newString = str.trim();
        int index = newString.indexOf("\r");
        if (index != -1) {
            return newString.substring(0, index);
        }

        index = newString.indexOf("\n");
        if (index != -1) {
            return newString.substring(0, index);
        }

        return newString;
    }

    public final String fetchNSAddr() {
        return fetchNSAddr(true, 3000);
    }

    public final String fetchNSAddr(boolean verbose, long timeoutMills) {
        String url = this.wsAddr;
        try {
            if (!UtilAll.isBlank(this.unitName)) {
                url = url + "-" + this.unitName + "?nofix=1";
            }
            HttpTinyClient.HttpResult result = HttpTinyClient.httpGet(url, null, null, "UTF-8", timeoutMills);
            if (200 == result.code) {
                String responseStr = result.content;
                if (responseStr != null) {
                    return clearNewLine(responseStr);
                } else {
                    log.error("fetch nameserver address is null");
                }
            } else {
                log.error("fetch nameserver address failed. statusCode={}", result.code);
            }
        } catch (IOException e) {
            if (verbose) {
                log.error("fetch name server address exception", e);
            }
        }

        if (verbose) {
            String errorMsg =
                    "connect to " + url + " failed, maybe the domain name " + MixAll.WS_DOMAIN_NAME + " not bind in /etc/hosts";
            errorMsg += FAQUrl.suggestTodo(FAQUrl.NAME_SERVER_ADDR_NOT_EXIST_URL);

            log.warn(errorMsg);
        }
        return null;
    }

    public String getNsAddr() {
        return nsAddr;
    }

    public void setNsAddr(String nsAddr) {
        this.nsAddr = nsAddr;
    }

}