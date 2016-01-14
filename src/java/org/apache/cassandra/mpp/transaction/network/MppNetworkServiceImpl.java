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
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.cassandra.mpp.transaction.MppMessageHandler;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 06/12/15
 */
public class MppNetworkServiceImpl implements MppNetworkService
{

    Logger logger = LoggerFactory.getLogger(MppNetworkServiceImpl.class);

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

    public void shutdown() throws Exception
    {
        mppNettyServer.shutdown();
        try
        {
            nettyEventLoopGroupsHolder.workerGroup.shutdownGracefully().get();
        }
        catch (InterruptedException | ExecutionException e)
        {
            e.printStackTrace();
        }
        try
        {
            nettyEventLoopGroupsHolder.bossGroup.shutdownGracefully().get();
        }
        catch (InterruptedException | ExecutionException e)
        {
            e.printStackTrace();
        }
    }

    private NettyEventLoopGroupsHolder nettyEventLoopGroupsHolder;

    private MppNettyClient mppNettyClient;

    private MppNettyServer mppNettyServer;

    private static class NettyEventLoopGroupsHolder
    {
        private EventLoopGroup bossGroup;
        private EventLoopGroup workerGroup;

        public EventLoopGroup getBossGroup()
        {
            return bossGroup;
        }

        public void setBossGroup(EventLoopGroup bossGroup)
        {
            this.bossGroup = bossGroup;
        }

        public EventLoopGroup getWorkerGroup()
        {
            return workerGroup;
        }

        public void setWorkerGroup(EventLoopGroup workerGroup)
        {
            this.workerGroup = workerGroup;
        }
    }

    // TODO [MPP] Handler useful if I do Request Response during single channel connection.
    // atm it works that client opens connection, writes, closes connection and response comes to server socket (mpp netty server)
    private static class NettyClientReadHandler extends ChannelInboundHandlerAdapter {

        final MppMessageConsumer mppMessageCallback;

        private NettyClientReadHandler(MppMessageConsumer mppMessageCallback)
        {
            this.mppMessageCallback = mppMessageCallback;
        }

        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
        {
            System.out.println("NettyClientReadHandler received message: " + msg);
            InetSocketAddress socketAddress = (InetSocketAddress) ctx.channel().remoteAddress();
            // TODO this allows to use like mppMessageConsumer.responseReceived
            mppMessageCallback.messageReceived((MppMessageEnvelope)msg, socketAddress);
            ctx.close();
        }

        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception
        {
            // 2
            super.channelUnregistered(ctx);
        }

        public void channelInactive(ChannelHandlerContext ctx) throws Exception
        {
            // 1
            super.channelInactive(ctx);
        }

        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception
        {
            super.channelReadComplete(ctx);
        }
    }

    private void initializeInternal()
    {
        initializeEventLoopGroups();
        initializeMppNettyServer();
        initializeMppNettyClient();
    }

    private void initializeEventLoopGroups()
    {
        nettyEventLoopGroupsHolder = new NettyEventLoopGroupsHolder();
        nettyEventLoopGroupsHolder.setBossGroup(new NioEventLoopGroup());
        nettyEventLoopGroupsHolder.setWorkerGroup(new NioEventLoopGroup());
    }

    private void initializeMppNettyClient()
    {
        mppNettyClient = new MppNettyClient(nettyEventLoopGroupsHolder.getWorkerGroup());
        mppNettyClient.init();
    }

    private void initializeMppNettyServer()
    {
        mppNettyServer = new MppNettyServer(nettyEventLoopGroupsHolder.bossGroup, nettyEventLoopGroupsHolder.workerGroup);
        try
        {
            mppNettyServer.start(listeningPort, (message, from) -> handleIncomingMessage(message, createReceipient(from.getAddress(), from.getPort())));
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }


    private static class AwaitingResponseMessageHolder<T>
    {
        MppMessageResponseExpectations<T> expectations;

        MppMessageResponseExpectations.MppMessageResponseDataHolder dataHolder;

        private AwaitingResponseMessageHolder(MppMessageResponseExpectations<T> expectations, MppMessageResponseExpectations.MppMessageResponseDataHolder dataHolder)
        {
            this.expectations = expectations;
            this.dataHolder = dataHolder;
        }
    }

    private AtomicLong messageIdGenerator = new AtomicLong(1);

    private Map<Long, AwaitingResponseMessageHolder> messageIdToResponseHolder = new ConcurrentHashMap<>();

    private MppMessageHandler messageExecutor;


    public void setMessageExecutor(MppMessageHandler messageExecutor)
    {
        this.messageExecutor = messageExecutor;
    }

    private <T> MppMessageEnvelope registerOutgoingMessage(MppMessage message, MppMessageResponseExpectations<T> mppMessageResponseExpectations,
                                                           MppMessageResponseExpectations.MppMessageResponseDataHolder dataHolder)
    {
        final long id = nextMessageId();
        messageIdToResponseHolder.put(id, new AwaitingResponseMessageHolder<T>(mppMessageResponseExpectations, dataHolder));
        return new MppMessageEnvelope(id, message, listeningPort);
    }

    private <T> void sendMessageOverNetwork(MppMessageEnvelope message, Collection<MessageReceipient> receipient) {
        receipient.forEach(r -> {
            final ChannelFuture channelFuture = mppNettyClient.connect(r.host(), r.port());
            try
            {
                final Channel channel = channelFuture.sync().channel();
//                if(message.getMessage().isRequest()) {
//                    channel.pipeline().addLast(new NettyClientReadHandler((response, from) -> {
//                        System.out.println("Handler got response " + response);
//                        handleIncomingMessage(response.getId(), response.getMessage(), createReceipient(from.getAddress(), from.getPort()));
//                    }));
//                }
                final ChannelFuture write = channel.writeAndFlush(message);
//                if(!message.getMessage().isRequest()) {
                    final ChannelFuture channelFuture1 = write.addListener(ChannelFutureListener.CLOSE);
//                }
                channel.closeFuture().sync();

            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        });
    }

    private long nextMessageId()
    {
        return messageIdGenerator.getAndIncrement();
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
            envelope = new MppMessageEnvelope(0, message, listeningPort);
        }
        sendMessageOverNetwork(envelope, receipients);
        return dataHolder != null ? dataHolder.getFuture() : null;
    }

    public void handleIncomingMessage(MppMessageEnvelope inEnv, MessageReceipient from)
    {
        long id = inEnv.getId();
        MppMessage incommingMessage = inEnv.getMessage();
        if (incommingMessage.isRequest())
        {
            final CompletableFuture<MppResponseMessage> executed = messageExecutor.handleMessage((MppRequestMessage) incommingMessage);
            if(incommingMessage.isResponseRequired()) {
                executed.thenAcceptAsync(response -> {
                    final MppMessageEnvelope envelope = new MppMessageEnvelope(id, response, listeningPort);
                    final int portForResponse = inEnv.getPortForResponse();
                    sendMessageOverNetwork(envelope, Collections.singleton(createReceipient(from.host(), portForResponse)));
                });
            }
        }
        else
        {
            // It is response to one of previous messages.
            final AwaitingResponseMessageHolder awaitingResponseMessageHolder = messageIdToResponseHolder.get(id);
            if(awaitingResponseMessageHolder == null) {
                logger.warn("Received message {}, but has no response message holder for it. It could be timed out or bug.", inEnv);
            }
            else {
                final MppMessageResponseExpectations.MppMessageResponseDataHolder dataHolder = awaitingResponseMessageHolder.dataHolder;
                boolean futureHasCompleted;
                synchronized (dataHolder) {
                    futureHasCompleted = awaitingResponseMessageHolder.expectations.maybeCompleteResponse(dataHolder, incommingMessage, createReceipient(from.host(), inEnv.getPortForResponse()));
                }

                if(futureHasCompleted) {
                    logger.info("Message with id {} has been handled and completed.", id);
                    unregisterResponseHolder(id);
                }
            }
        }
    }

    public MessageReceipient createReceipient(InetAddress addr)
    {
        // TODO [MPP] get port from netty service.
        throw new NotImplementedException();
//        return createReceipient(addr, 0);

    }

    public MessageReceipient createReceipient(InetAddress addr, int port)
    {
        return new MessageReceipient()
        {
            public InetAddress host()
            {
                return addr;
            }

            public int port()
            {
                return port;
            }
        };
    }

    private void unregisterResponseHolder(long id)
    {
        messageIdToResponseHolder.remove(id);
    }
}
