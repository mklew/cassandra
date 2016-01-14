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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

/**
* @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
* @since 14/01/16
*/
class MppNettyServer
{

    EventLoopGroup bossGroup;
    EventLoopGroup workerGroup;
    ChannelFuture socketChannel;

    public MppNettyServer(EventLoopGroup bossGroup, EventLoopGroup workerGroup)
    {
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
    }

    void start(int listenOnPort, MppMessageConsumer mppMessageCallback) throws InterruptedException
    {
        ServerBootstrap b = new ServerBootstrap(); // (2)
        b.group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class) // (3)
        .childHandler(new ChannelInitializer<SocketChannel>()
        { // (4)
            @Override
            public void initChannel(SocketChannel ch) throws Exception
            {
                ch.pipeline().addLast(new ObjectEncoder());
                ch.pipeline().addLast(new ObjectDecoder(ClassResolvers.softCachingConcurrentResolver(getClass().getClassLoader())));
                ch.pipeline().addLast(new MppNettyServerChannelHandler(mppMessageCallback));
            }
        })
        .option(ChannelOption.SO_BACKLOG, 128)          // (5)
        .childOption(ChannelOption.SO_KEEPALIVE, true); // (6)

        // Bind and start to accept incoming connections.
        socketChannel = b.bind(listenOnPort).sync(); // (7)


        // Wait until the server socket is closed.
        // In this example, this does not happen, but you can do that to gracefully
        // shut down your server.
//                f.channel().closeFuture().sync();
    }

    void shutdown() throws Exception {
        final NioServerSocketChannel channel = (NioServerSocketChannel) socketChannel.sync().channel();
        channel.close().sync();
    }

}
