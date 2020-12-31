/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.remoting.common;

/**
 * @author xuleyan
 * @version Pair.java, v 0.1 2020-10-10 11:30 上午
 */
public class Pair<T1, T2> {
    private T1 object1;
    private T2 object2;

    public Pair(T1 object1, T2 object2) {
        this.object1 = object1;
        this.object2 = object2;
    }

    public T1 getObject1() {
        return object1;
    }

    public void setObject1(T1 object1) {
        this.object1 = object1;
    }

    public T2 getObject2() {
        return object2;
    }

    public void setObject2(T2 object2) {
        this.object2 = object2;
    }
}