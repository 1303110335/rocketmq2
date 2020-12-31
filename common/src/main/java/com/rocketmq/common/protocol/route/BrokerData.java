/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.common.protocol.route;

import java.util.HashMap;

/**
 * @author xuleyan
 * @version brokerData.java, v 0.1 2020-10-10 10:04 上午
 */
public class BrokerData implements Comparable<BrokerData> {

    /**
     * 集群名
     */
    private String cluster;
    /**
     * broker名称
     */
    private String brokerName;

    /**
     * 存放broker服务器的地址
     */
    private HashMap<Long/* brokerId */, String/* brokerAddress */> brokerAddrs;

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public HashMap<Long, String> getBrokerAddrs() {
        return brokerAddrs;
    }

    public void setBrokerAddrs(HashMap<Long, String> brokerAddrs) {
        this.brokerAddrs = brokerAddrs;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((brokerAddrs == null) ? 0 : brokerAddrs.hashCode());
        result = prime * result + ((brokerName == null) ? 0 : brokerName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        BrokerData other = (BrokerData) obj;
        if (brokerAddrs == null) {
            if (other.brokerAddrs != null) {
                return false;
            }
        } else if (brokerAddrs != other.brokerAddrs) {
            return false;
        }
        if (brokerName == null) {
            if (other.brokerName != null) {
                return false;
            }
        } else if (!brokerName.equals(other.brokerName)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "BrokerData [brokerName=" + brokerName + ", brokerAddrs=" + brokerAddrs + "]";
    }

    @Override
    public int compareTo(BrokerData o) {
        return this.brokerName.compareTo(o.brokerName);
    }
}