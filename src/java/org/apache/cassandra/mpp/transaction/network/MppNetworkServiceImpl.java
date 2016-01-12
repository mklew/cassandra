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

import java.net.InetAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.cassandra.mpp.transaction.MppMessageExecutor;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 06/12/15
 */
public class MppNetworkServiceImpl implements MppNetworkService
{

    private int listeningPort;

    public void setListeningPort(int listeningPort)
    {
        this.listeningPort = listeningPort;
    }

    public void initialize()
    {
        assert listeningPort != 0;
        assert messageExecutor != null;
        initializeInternal();
    }

    public void shutdown()
    {
        nettyServer.shutdown();
    }

    private NettyServer nettyServer;

    private static class NettyServer {

        EventLoopGroup bossGroup;
        EventLoopGroup workerGroup;

        void start(int listenOnPort) throws InterruptedException
        {
            bossGroup = new NioEventLoopGroup(); // (1)
            workerGroup = new NioEventLoopGroup();
            ServerBootstrap b = new ServerBootstrap(); // (2)
            b.group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class) // (3)
            .childHandler(new ChannelInitializer<SocketChannel>() { // (4)
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
//                        ch.pipeline().addLast(new ObjectEncoder());
//                        ch.pipeline().addLast(new ObjectDecoder(ClassResolvers.softCachingConcurrentResolver(getClass().getClassLoader())));
//                        ch.pipeline().addLast(new ObjectLogicHandler());
                }
            })
            .option(ChannelOption.SO_BACKLOG, 128)          // (5)
            .childOption(ChannelOption.SO_KEEPALIVE, true); // (6)

            // Bind and start to accept incoming connections.
            ChannelFuture f = b.bind(listenOnPort).sync(); // (7)

            // Wait until the server socket is closed.
            // In this example, this does not happen, but you can do that to gracefully
            // shut down your server.
//                f.channel().closeFuture().sync();
        }

        void shutdown() {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }

    }

    private void initializeInternal()
    {
        nettyServer = new NettyServer();
        try
        {
            nettyServer.start(listeningPort);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static class ResponseHolder<T>
    {
        MppMessageResponseExpectations<T> expectations;

        MppMessageResponseExpectations.MppMessageResponseDataHolder dataHolder;

        private ResponseHolder(MppMessageResponseExpectations<T> expectations, MppMessageResponseExpectations.MppMessageResponseDataHolder dataHolder)
        {
            this.expectations = expectations;
            this.dataHolder = dataHolder;
        }
    }

    private AtomicLong idGen = new AtomicLong(1);

    private Map<Long, ResponseHolder> idToResponseHolder = new ConcurrentHashMap<>();

    private MppMessageExecutor messageExecutor;


    public void setMessageExecutor(MppMessageExecutor messageExecutor)
    {
        this.messageExecutor = messageExecutor;
    }

    private <T> MppMessageEnvelope registerOutgoingMessage(MppMessage message, MppMessageResponseExpectations<T> mppMessageResponseExpectations,
                                                           MppMessageResponseExpectations.MppMessageResponseDataHolder dataHolder)
    {
        final long id = nextId();
        idToResponseHolder.put(id, new ResponseHolder<T>(mppMessageResponseExpectations, dataHolder));
        return new MppMessageEnvelope(id, message);
    }

    private <T>  void sendMessageOverNetwork(MppMessageEnvelope message, MppMessageResponseExpectations<T> mppMessageResponseExpectations, Collection<MessageReceipient> receipient) {
        // TODO [MPP] implementation over netty.
        // TODO [MPP] handle timeout
        // TODO [MPP] Just open channel; send; close channel ?
    }

    private long nextId()
    {
        return idGen.getAndIncrement();
    }

    public <T> CompletableFuture<T> sendMessage(MppMessage message,
                                                MppMessageResponseExpectations<T> mppMessageResponseExpectations,
                                                Collection<MessageReceipient> receipients)
    {
        MppMessageResponseExpectations.MppMessageResponseDataHolder dataHolder = null;
        final MppMessageEnvelope envelope;
        if (mppMessageResponseExpectations.expectsResponse())
        {
            dataHolder = mppMessageResponseExpectations.createDataHolder(message, receipients);
            envelope = registerOutgoingMessage(message, mppMessageResponseExpectations, dataHolder);
        }
        else {
            envelope = new MppMessageEnvelope(0, message);
        }
        sendMessageOverNetwork(envelope, mppMessageResponseExpectations, receipients);
        return dataHolder != null ? dataHolder.getFuture() : null;
    }

    public void handleIncomingMessage(long id, MppMessage incommingMessage, MessageReceipient from)
    {
        if (incommingMessage.isRequest())
        {
            messageExecutor.executeRequest((MppRequestMessage) incommingMessage).thenAcceptAsync(response -> {
                final MppMessageEnvelope envelope = new MppMessageEnvelope(id, response);
                sendMessageOverNetwork(envelope, MppMessageResponseExpectations.NO_MPP_MESSAGE_RESPONSE, Collections.singleton(from));
            });
        }
        else
        {
            // It is response to one of previous messages.
            final ResponseHolder responseHolder = idToResponseHolder.get(id);
            if(responseHolder == null) {
                // TODO [MPP] It can be null if timeout has occured and response holder was already removed.
                // TODO [MPP] It can be bug
                // TODO [MPP] Log this message.
            }
            else {
                final MppMessageResponseExpectations.MppMessageResponseDataHolder dataHolder = responseHolder.dataHolder;
                boolean futureHasCompleted;
                synchronized (dataHolder) {
                    futureHasCompleted = responseHolder.expectations.maybeCompleteResponse(dataHolder, incommingMessage, from);
                }

                if(futureHasCompleted) {
                    // TODO [MPP] Log that message with ID has completed response.
                    unregisterResponseHolder(id);
                }
            }
        }
    }

    public MessageReceipient createReceipient(InetAddress addr)
    {
        return new MessageReceipient()
        {
            public InetAddress host()
            {
                return addr;
            }

            public int port()
            {
                // TODO [MPP] get port from netty service.
                throw new NotImplementedException();
            }
        };
    }

    private void unregisterResponseHolder(long id)
    {
        idToResponseHolder.remove(id);
    }
}
