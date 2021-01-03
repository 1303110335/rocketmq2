/**
 * xuleyan
 * Copyright (C) 2013-2021 All Rights Reserved.
 */
package com.rocketmq.client.latency;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author xuleyan
 * @version LatencyFaultToleranceImpl.java, v 0.1 2021-01-02 5:04 PM xuleyan
 */
public class LatencyFaultToleranceImpl implements LatencyFaultTolerance<String> {

    /**
     * 对象故障信息Table
     */
    private final ConcurrentHashMap<String, FaultItem> faultItemTable = new ConcurrentHashMap<>(16);

    @Override
    public void updateFaultItem(String name, long currentLatency, long notAvailableDuration) {

    }

    @Override
    public boolean isAvailable(String name) {
        return false;
    }

    @Override
    public void remove(String name) {

    }

    @Override
    public String pickOneAtLeast() {
        return null;
    }

    class FaultItem implements Comparable<FaultItem> {

        /**
         * 对象名
         */
        private final String name;
        /**
         * 延迟
         */
        private volatile long currentLatency;
        /**
         * 开始可用时间
         */
        private volatile long startTimestamp;

        public FaultItem(String name) {
            this.name = name;
        }

        /**
         * 比较对象
         * 可用性 > 延迟 > 开始可用时间
         *
         * @param other other
         * @return 升序
         */
        @Override
        public int compareTo(final FaultItem other) {
            if (this.isAvailable() != other.isAvailable()) {
                if (this.isAvailable())
                    return -1;

                if (other.isAvailable())
                    return 1;
            }

            if (this.currentLatency < other.currentLatency)
                return -1;
            else if (this.currentLatency > other.currentLatency) {
                return 1;
            }

            if (this.startTimestamp < other.startTimestamp)
                return -1;
            else if (this.startTimestamp > other.startTimestamp) {
                return 1;
            }

            return 0;
        }

        /**
         * 是否可用：当开始可用时间大于当前时间
         *
         * @return 是否可用
         */
        public boolean isAvailable() {
            return (System.currentTimeMillis() - startTimestamp) >= 0;
        }

        @Override
        public int hashCode() {
            int result = getName() != null ? getName().hashCode() : 0;
            result = 31 * result + (int) (getCurrentLatency() ^ (getCurrentLatency() >>> 32));
            result = 31 * result + (int) (getStartTimestamp() ^ (getStartTimestamp() >>> 32));
            return result;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (!(o instanceof FaultItem))
                return false;

            final FaultItem faultItem = (FaultItem) o;

            if (getCurrentLatency() != faultItem.getCurrentLatency())
                return false;
            if (getStartTimestamp() != faultItem.getStartTimestamp())
                return false;
            return getName() != null ? getName().equals(faultItem.getName()) : faultItem.getName() == null;

        }

        @Override
        public String toString() {
            return "FaultItem{" +
                    "name='" + name + '\'' +
                    ", currentLatency=" + currentLatency +
                    ", startTimestamp=" + startTimestamp +
                    '}';
        }

        public String getName() {
            return name;
        }

        public long getCurrentLatency() {
            return currentLatency;
        }

        public void setCurrentLatency(final long currentLatency) {
            this.currentLatency = currentLatency;
        }

        public long getStartTimestamp() {
            return startTimestamp;
        }

        public void setStartTimestamp(final long startTimestamp) {
            this.startTimestamp = startTimestamp;
        }

    }
}