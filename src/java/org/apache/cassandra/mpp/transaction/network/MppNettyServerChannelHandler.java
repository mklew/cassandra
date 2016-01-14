/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.mpp.transaction.network;

import java.net.InetSocketAddress;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
* @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
* @since 14/01/16
*/
class MppNettyServerChannelHandler extends ChannelInboundHandlerAdapter
{
    final MppMessageConsumer mppMessageConsumer;

    MppNettyServerChannelHandler(MppMessageConsumer mppMessageConsumer)
    {
        this.mppMessageConsumer = mppMessageConsumer;
    }

    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
    {
        InetSocketAddress socketAddress = (InetSocketAddress) ctx.channel().remoteAddress();
        mppMessageConsumer.messageReceived((MppMessageEnvelope) msg, socketAddress);
    }

    public void channelActive(ChannelHandlerContext ctx) throws Exception
    {
        System.out.println("NettyServerChannelHandler is active");
    }
}
