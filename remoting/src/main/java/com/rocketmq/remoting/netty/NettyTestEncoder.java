/**
 * bianque.com
 * Copyright (C) 2013-2020 All Rights Reserved.
 */
package com.rocketmq.remoting.netty;

import com.rocketmq.remoting.RemotingCommand;
import com.rocketmq.remoting.common.RemotingHelper;
import com.rocketmq.remoting.common.RemotingUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import lombok.extern.slf4j.Slf4j;

/**
 * @author xuleyan
 * @version NettyTestEncoder.java, v 0.1 2020-11-30 10:09 上午
 */
@Slf4j
public class NettyTestEncoder extends MessageToByteEncoder<RemotingCommand> {

    @Override
    protected void encode(ChannelHandlerContext ctx, RemotingCommand remotingCommand, ByteBuf out) throws Exception {
        try {
            byte[] body = remotingCommand.getBody();
            if (body != null) {
                out.writeBytes(body);
            }
        } catch (Exception e) {
            log.error("encode com.rocketmq.client.exception, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()), e);
            if (remotingCommand != null) {
                log.error(remotingCommand.toString());
            }
            RemotingUtil.closeChannel(ctx.channel());
        }
    }
}