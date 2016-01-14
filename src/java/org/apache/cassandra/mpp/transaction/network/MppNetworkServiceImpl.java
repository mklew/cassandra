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

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
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

    public void shutdown() throws Exception
    {
        nettyServer.shutdown();
        try
        {
            nettyEventLoopGroupsHolder.workerGroup.shutdownGracefully().get();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        catch (ExecutionException e)
        {
            e.printStackTrace();
        }
        try
        {
            nettyEventLoopGroupsHolder.bossGroup.shutdownGracefully().get();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        catch (ExecutionException e)
        {
            e.printStackTrace();
        }
    }

    private NettyEventLoopGroupsHolder nettyEventLoopGroupsHolder;

    private NettyClient nettyClient;

    private NettyServer nettyServer;

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

    private static class NettyClientHandler extends ChannelOutboundHandlerAdapter
    {
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception
        {
            super.write(ctx, msg, promise);
        }

        public void read(ChannelHandlerContext ctx) throws Exception
        {
            super.read(ctx);
        }
    }

    private static class NettyClientReadHandler extends ChannelInboundHandlerAdapter {

        final ChannelMessageCallback mppMessageCallback;

        private NettyClientReadHandler(ChannelMessageCallback mppMessageCallback)
        {
            this.mppMessageCallback = mppMessageCallback;
        }

        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
        {
            System.out.println("NettyClientReadHandler received message: " + msg);
            InetSocketAddress socketAddress = (InetSocketAddress) ctx.channel().remoteAddress();
            // TODO this allows to use like mppMessageCallback.responseReceived
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

    private static class NettyClient {

        private final EventLoopGroup workerGroup;

        private Bootstrap b;

        private NettyClient(EventLoopGroup workerGroup)
        {
            this.workerGroup = workerGroup;
        }

        void init() {
            b = new Bootstrap(); // (1)
            b.group(workerGroup); // (2)
            b.channel(NioSocketChannel.class); // (3)
            b.option(ChannelOption.SO_KEEPALIVE, true); // (4)
            b.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(new ObjectEncoder());
                    ch.pipeline().addLast(new ObjectDecoder(ClassResolvers.softCachingConcurrentResolver(getClass().getClassLoader())));
                    ch.pipeline().addLast(new NettyClientHandler());
                }
            });

                // Start the client.
//                final ChannelFuture connect = b.connect(InetAddress.getLocalHost(), portToConnect);
//                final boolean await = connect.await(waitMillis, TimeUnit.MILLISECONDS);// (5)
//                doWithChannel.accept(connect);
//                System.out.println("isSuccess" + connect.isSuccess());
//                connect.channel().closeFuture().sync();

//            } finally {
//                workerGroup.shutdownGracefully();
//            }
        }

        ChannelFuture connect(InetAddress host, int port) {
            return b.connect(host, port);
        }
    }

    private interface ChannelMessageCallback {
        void messageReceived(MppMessageEnvelope message, InetSocketAddress from);
    }

    private static class NettyServerChannelHandler extends ChannelInboundHandlerAdapter {

        final ChannelMessageCallback mppMessageCallback;

        private NettyServerChannelHandler(ChannelMessageCallback mppMessageCallback)
        {
            this.mppMessageCallback = mppMessageCallback;
        }

        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
        {
            System.out.println("NettyServerChannelHandler received message: " + msg); // TODO remove that
            InetSocketAddress socketAddress = (InetSocketAddress) ctx.channel().remoteAddress();
            mppMessageCallback.messageReceived((MppMessageEnvelope)msg, socketAddress);
        }

        public void channelActive(ChannelHandlerContext ctx) throws Exception
        {
            System.out.println("NettyServerChannelHandler is active");
        }
    }

    private static class NettyServer {

        EventLoopGroup bossGroup;
        EventLoopGroup workerGroup;
        ChannelFuture socketChannel;

        public NettyServer(EventLoopGroup bossGroup, EventLoopGroup workerGroup)
        {
            this.bossGroup = bossGroup;
            this.workerGroup = workerGroup;
        }

        void start(int listenOnPort, ChannelMessageCallback mppMessageCallback) throws InterruptedException
        {
            ServerBootstrap b = new ServerBootstrap(); // (2)
            b.group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class) // (3)
            .childHandler(new ChannelInitializer<SocketChannel>() { // (4)
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new ObjectEncoder());
                        ch.pipeline().addLast(new ObjectDecoder(ClassResolvers.softCachingConcurrentResolver(getClass().getClassLoader())));
                        ch.pipeline().addLast(new NettyServerChannelHandler(mppMessageCallback));
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

    private void initializeInternal()
    {
        nettyEventLoopGroupsHolder = new NettyEventLoopGroupsHolder();

        nettyEventLoopGroupsHolder.setBossGroup(new NioEventLoopGroup());
        nettyEventLoopGroupsHolder.setWorkerGroup(new NioEventLoopGroup());

        nettyServer = new NettyServer(nettyEventLoopGroupsHolder.bossGroup, nettyEventLoopGroupsHolder.workerGroup);
        try
        {
            nettyServer.start(listeningPort, (message, from) -> handleIncomingMessage(message, createReceipient(from.getAddress(), from.getPort())));
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }

        nettyClient = new NettyClient(nettyEventLoopGroupsHolder.getWorkerGroup());
        nettyClient.init();
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
        return new MppMessageEnvelope(id, message, listeningPort);
    }

    private <T> void sendMessageOverNetwork(MppMessageEnvelope message, Collection<MessageReceipient> receipient) {
        receipient.forEach(r -> {
            final ChannelFuture channelFuture = nettyClient.connect(r.host(), r.port());
            try
            {
                System.out.println("Writing and flushing message" + message);
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
            envelope = new MppMessageEnvelope(0, message, listeningPort);
        }
        sendMessageOverNetwork(envelope, receipients);
        return dataHolder != null ? dataHolder.getFuture() : null;
    }

    public void handleIncomingMessage(MppMessageEnvelope inEnv, MessageReceipient from)
    {
        long id = inEnv.getId();
        MppMessage incommingMessage = inEnv.getMessage();
        System.out.println("handleIncomingMessage id " + id +  ", message" + incommingMessage + ", from " + from);
        if (incommingMessage.isRequest())
        {
            messageExecutor.executeRequest((MppRequestMessage) incommingMessage).thenAcceptAsync(response -> {
                final MppMessageEnvelope envelope = new MppMessageEnvelope(id, response, listeningPort);
                final int portForResponse = inEnv.getPortForResponse();
                sendMessageOverNetwork(envelope, Collections.singleton(createReceipient(from.host(), portForResponse)));
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
                    futureHasCompleted = responseHolder.expectations.maybeCompleteResponse(dataHolder, incommingMessage, createReceipient(from.host(), inEnv.getPortForResponse()));
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
        idToResponseHolder.remove(id);
    }
}
