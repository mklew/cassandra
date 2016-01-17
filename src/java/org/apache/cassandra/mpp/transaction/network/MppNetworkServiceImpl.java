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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

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
import org.apache.cassandra.utils.Pair;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 06/12/15
 */
public class MppNetworkServiceImpl implements MppNetworkService
{

    Logger logger = LoggerFactory.getLogger(MppNetworkServiceImpl.class);

    private enum State
    {
        NOT_STARTED, RUNNING, SHUTDOWN
    }

    private int listeningPort;

    private static final long SHUTDOWN_TIMEOUT_MS = 100;
    private static final long SHUTDOWN_GRACE_PERIOD_TIMEOUT_MS = 50;

    private static final long TIMEOUT_TO_HANDLE_MESSAGE_MS = 5_000;

    private long defaultTimeout = 250;
    private MppNetworkHooks hooks;
    private String name;

    private State state = State.NOT_STARTED;

    public void setListeningPort(int listeningPort)
    {
        this.listeningPort = listeningPort;
    }

    public void initialize()
    {
        assert state == State.NOT_STARTED;
        assert listeningPort != 0;
        assert messageHandler != null;
        initializeInternal();
    }

    public void shutdown() throws Exception
    {
        if (state == State.SHUTDOWN)
        {
            return;
        }
        assert state == State.RUNNING;
        logger.info("Shutting down netty server {}", name);
        mppNettyServer.shutdown();
        try
        {
            logger.info("Shutting down workerGroup {}", name);
            // TODO [MPP] timeout on GET and then do just shutdown if it hasn't been shut already.
            nettyEventLoopGroupsHolder.workerGroup.shutdownGracefully(SHUTDOWN_GRACE_PERIOD_TIMEOUT_MS, SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS).get();
            logger.info("WorkerGroup has been shutdown {}", name);
        }
        catch (InterruptedException | ExecutionException e)
        {
            e.printStackTrace();
        }
        try
        {
            logger.info("Shutting bossGroup has been shutdown {}", name);
            nettyEventLoopGroupsHolder.bossGroup.shutdownGracefully(SHUTDOWN_GRACE_PERIOD_TIMEOUT_MS, SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS).get();
            logger.info("BossGroup has been shutdown {}", name);
        }
        catch (InterruptedException | ExecutionException e)
        {
            e.printStackTrace();
        }
        state = State.SHUTDOWN;
    }

    private NettyEventLoopGroupsHolder nettyEventLoopGroupsHolder;

    private MppNettyClient mppNettyClient;

    private MppNettyServer mppNettyServer;

    private Integer limitNumberOfEventLoopThreads;

    public void setLimitNumberOfEventLoopThreads(Integer limitNumberOfEventLoopThreads)
    {
        this.limitNumberOfEventLoopThreads = limitNumberOfEventLoopThreads;
    }

    public void setDefaultTimeout(long defaultTimeout)
    {
        this.defaultTimeout = defaultTimeout;
    }

    public long getDefaultTimeout()
    {
        return defaultTimeout;
    }

    public void setHooks(MppNetworkHooks hooks)
    {
        this.hooks = hooks;
    }

    public MppNetworkHooks getHooks()
    {
        return hooks;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getName()
    {
        return name;
    }

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
        if(limitNumberOfEventLoopThreads != null) {
            nettyEventLoopGroupsHolder.setBossGroup(new NioEventLoopGroup(limitNumberOfEventLoopThreads));
            nettyEventLoopGroupsHolder.setWorkerGroup(new NioEventLoopGroup(limitNumberOfEventLoopThreads));
        }
        else {
            nettyEventLoopGroupsHolder.setBossGroup(new NioEventLoopGroup());
            nettyEventLoopGroupsHolder.setWorkerGroup(new NioEventLoopGroup());
        }
    }

    private void initializeMppNettyClient()
    {
        logger.info("Starting netty client {}", name);
        mppNettyClient = new MppNettyClient(nettyEventLoopGroupsHolder.getWorkerGroup());
        mppNettyClient.init();
    }

    private void initializeMppNettyServer()
    {
        logger.info("Starting netty server {}", name);
        mppNettyServer = new MppNettyServer(nettyEventLoopGroupsHolder.bossGroup, nettyEventLoopGroupsHolder.workerGroup);
        try
        {
            mppNettyServer.start(listeningPort, (message, from) -> handleIncomingMessage(message, createReceipient(from.getAddress(), from.getPort())));
            state = State.RUNNING;
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }


    private static class AwaitingResponseMessageHolder<T>
    {
        Logger logger = LoggerFactory.getLogger(getClass());

        MppMessageResponseExpectations<T> expectations;

        MppMessageResponseExpectations.MppMessageResponseDataHolder dataHolder;

        private final ReentrantLock lock = new ReentrantLock();

        private AwaitingResponseMessageHolder(MppMessageResponseExpectations<T> expectations, MppMessageResponseExpectations.MppMessageResponseDataHolder dataHolder)
        {
            this.expectations = expectations;
            this.dataHolder = dataHolder;
        }

        public boolean maybeCompleteResponse(MppMessage incommingMessage, MessageReceipient receipient)
        {
            logger.debug("maybeCompleteResponse {} lock", Thread.currentThread());
            lock.lock();
            try {
                logger.debug("maybeCompleteResponse {} lock acquired", Thread.currentThread());
                return expectations.maybeCompleteResponse(dataHolder, incommingMessage, receipient);
            }
            finally
            {
                logger.debug("maybeCompleteResponse {} lock unlocked", Thread.currentThread());
                lock.unlock();
            }
        }

        public void timeout(long messageId, MessageReceipient r)
        {
            lock.lock();
            try {
                expectations.timeoutHasOccurred(dataHolder, messageId, r);
            }
            finally
            {
                lock.unlock();
            }
        }
    }

    private AtomicLong messageIdGenerator = new AtomicLong(1);

    private Map<Long, AwaitingResponseMessageHolder> messageIdToResponseHolder = new ConcurrentHashMap<>();

    private MppMessageHandler messageHandler;


    public void setMessageHandler(MppMessageHandler messageExecutor)
    {
        this.messageHandler = messageExecutor;
    }

    private <T> MppMessageEnvelope registerOutgoingMessage(MppMessage message, MppMessageResponseExpectations<T> mppMessageResponseExpectations,
                                                           MppMessageResponseExpectations.MppMessageResponseDataHolder dataHolder)
    {
        final long id = nextMessageId();
        messageIdToResponseHolder.put(id, new AwaitingResponseMessageHolder<T>(mppMessageResponseExpectations, dataHolder));
        return new MppMessageEnvelope(id, message, listeningPort);
    }

    private List<CompletableFuture<Pair<MessageReceipient, Optional<Throwable>>>> sendMessageOverNetwork(MppMessageEnvelope message, Collection<MessageReceipient> receipient) {
        final List<CompletableFuture<Pair<MessageReceipient, Optional<Throwable>>>> listOfFutures = receipient.stream().map(r -> {
            CompletableFuture<Pair<MessageReceipient, Optional<Throwable>>> futureSuccessOrFailure = new CompletableFuture<>();
            if (hooks != null)
            {
                hooks.outgoingMessageBeforeSending(message, r);
            }
            final long messageId = message.getId();
            final ChannelFuture channelFuture = mppNettyClient.connect(r.host(), r.port());
            channelFuture.addListener(new ChannelFutureListener()
            {
                public void operationComplete(ChannelFuture f) throws Exception
                {
                    if (!f.isSuccess())
                    {
                        hooks.cannotConnectToReceipient(messageId, r, f.cause());
                        completeWithThrowable(r, futureSuccessOrFailure, f.cause());
                    }
                }
            });
            final Channel channel;
            try
            {
                channel = channelFuture.sync().channel();
                //                if(message.getMessage().isRequest()) {
//                    channel.pipeline().addLast(new NettyClientReadHandler((response, from) -> {
//                        System.out.println("Handler got response " + response);
//                        handleIncomingMessage(response.getId(), response.getMessage(), createReceipient(from.getAddress(), from.getPort()));
//                    }));
//                }
                final ChannelFuture write = channel.writeAndFlush(message);
//                if(!message.getMessage().isRequest()) {
                completeFutureWithSuccessfulSend(r, futureSuccessOrFailure, write);
                final ChannelFuture channelFuture1 = write.addListener(ChannelFutureListener.CLOSE);
//                write.addListener(future -> {
//                    if(hooks != null) {
//                        hooks.outgoingMessageHasBeenSent(message, r);
//                    }
//                });
//                }
                addTimeoutListener(r, messageId, write);
                channel.closeFuture().sync();
            }
            catch (Exception e)
            {
                if (hooks != null)
                {
                    hooks.cannotConnectToReceipient(messageId, r, e);
                    completeWithThrowable(r, futureSuccessOrFailure, e);
                }
            }
            return futureSuccessOrFailure;
        }).collect(Collectors.toList());

        return listOfFutures;
    }

    private void addTimeoutListener(MessageReceipient r, long messageId, ChannelFuture write)
    {
        write.addListener(f -> {
            nettyEventLoopGroupsHolder.getWorkerGroup().schedule((Runnable) () -> {
                messageHasTimedOut(messageId, r);
            }, getDefaultTimeout(), TimeUnit.MILLISECONDS);
        });
    }

    private static void completeFutureWithSuccessfulSend(MessageReceipient r, CompletableFuture<Pair<MessageReceipient, Optional<Throwable>>> futureSuccessOrFailure, ChannelFuture write)
    {
        write.addListener(future -> {
            futureSuccessOrFailure.complete(Pair.create(r, Optional.empty()));
        });
    }

    private static void completeWithThrowable(MessageReceipient r, CompletableFuture<Pair<MessageReceipient, Optional<Throwable>>> futureSuccessOrFailure, Throwable e)
    {
        futureSuccessOrFailure.complete(Pair.create(r, Optional.of(e)));
    }

    private long nextMessageId()
    {
        return messageIdGenerator.getAndIncrement();
    }


    @Override
    public <T> MessageResult<T> sendMessage(MppMessage message,
                                            MppMessageResponseExpectations<T> mppMessageResponseExpectations,
                                            Collection<MessageReceipient> receipients)
    {
        MppMessageResponseExpectations.MppMessageResponseDataHolder<T> dataHolder = null;
        final MppMessageEnvelope envelope;
        if (mppMessageResponseExpectations.expectsResponse())
        {
            dataHolder = mppMessageResponseExpectations.createDataHolder(message, receipients);
            envelope = registerOutgoingMessage(message, mppMessageResponseExpectations, dataHolder);
        }
        else {
            envelope = new MppMessageEnvelope(0, message, listeningPort);
        }
        final List<CompletableFuture<Pair<MessageReceipient, Optional<Throwable>>>> futureOfMessageSentIntoNetwork = sendMessageOverNetwork(envelope, receipients);

        if(dataHolder != null) {
            return new MessageResult<T>(dataHolder.getFuture(), futureOfMessageSentIntoNetwork);
        }
        else {
            return new MessageResult<T>(null, futureOfMessageSentIntoNetwork);
        }
    }

    private void messageHasTimedOut(long messageId, MessageReceipient r)
    {
        final AwaitingResponseMessageHolder awaitingResponseMessageHolder = messageIdToResponseHolder.get(messageId);
        awaitingResponseMessageHolder.timeout(messageId, r);
        unregisterResponseHolder(messageId);
        hooks.messageHasTimedOut(messageId, r);
    }

    public void handleIncomingMessage(MppMessageEnvelope inEnv, MessageReceipient from)
    {
        long id = inEnv.getId();
        MppMessage incommingMessage = inEnv.getMessage();
        if(hooks != null) {
            hooks.incomingMessage(inEnv, from);
        }
        if (incommingMessage.isRequest())
        {
            final CompletableFuture<MppResponseMessage> executed = messageHandler.handleMessage((MppRequestMessage) incommingMessage);
            if(incommingMessage.isResponseRequired()) {
                try
                {
                    final MppResponseMessage response;
                    try
                    {
                        response = executed.get(TIMEOUT_TO_HANDLE_MESSAGE_MS, TimeUnit.MILLISECONDS);
                        final MppMessageEnvelope envelope = new MppMessageEnvelope(id, response, listeningPort);
                        final int portForResponse = inEnv.getPortForResponse();
                        final List<CompletableFuture<Pair<MessageReceipient, Optional<Throwable>>>> completableFutures = sendMessageOverNetwork(envelope, Collections.singleton(createReceipient(from.host(), portForResponse)));
                        // TODO [MPP] It might assert that response message has been sent correctly.
                    }
                    catch (TimeoutException e)
                    {
                        if(hooks != null) {
                            hooks.failedToExecuteIncomingMessageInTime(inEnv.getId(), from);
                        }
                    }
                }
                catch (InterruptedException | ExecutionException e)
                {
                    throw new RuntimeException(e);
                }
//                responseMessage.thenAcceptAsync(

//                response -> {

//                });
            }
        }
        else
        {
            // It is response to one of previous messages.
            logger.info("Message with id {} tries to be completed. NS {} Thread {}", id, name, Thread.currentThread());
            logger.info("Message with id {} before get. NS {} Thread {}", id, name, Thread.currentThread());
            final AwaitingResponseMessageHolder awaitingResponseMessageHolder = messageIdToResponseHolder.get(id);
            logger.info("Message with id {} AFTER get. NS {} Thread {}", id, name, Thread.currentThread());
            if(awaitingResponseMessageHolder == null) {
                logger.warn("Received message {}, but has no response message holder for it. It could be timed out or bug.", inEnv);
            }
            else {
                boolean futureHasCompleted = awaitingResponseMessageHolder.maybeCompleteResponse(incommingMessage, createReceipient(from.host(), inEnv.getPortForResponse()));
                logger.debug("MessageId {} synchronize data holder. NS {} ", id, name);
                if(futureHasCompleted) {
                    logger.info("Message with id {} has been handled and completed.", id);
                    unregisterResponseHolder(id);
                    if(hooks != null) {
                        hooks.messageHasBeenHandledSuccessfully(inEnv.getId(), Collections.singletonList(from));
                    }
                }
                else {
                    logger.debug("MessageId {} has not completed yet. NS {} ", id, name);
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

    public String toString()
    {
        return name + ':' + listeningPort;
    }
}
