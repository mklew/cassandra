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
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Test;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.cassandra.mpp.transaction.MppMessageHandler;
import org.apache.cassandra.mpp.transaction.NodeContext;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 12/01/16
 */
public class MppNetworkServiceImplTest
{
    private static class TestMessageHandler implements MppMessageHandler
    {

        public CompletableFuture<MppResponseMessage> handleMessage(MppRequestMessage requestMessage)
        {
            return null;
        }
    }

    int ns1Port = 50001;

    int ns2Port = 50002;

    @Test
    public void testCreateMppNetworkService() throws Exception
    {
        final MppNetworkServiceImpl ns1 = setupNs1();
        final MppNetworkServiceImpl ns2 = setupNs2();
        ns1.initialize();
        ns2.initialize();

        ns1.shutdown();
        ns2.shutdown();
    }

    @Test
    public void testShouldBeAbleToConnectAfterInitilized() throws Exception
    {
        final MppNetworkServiceImpl ns1 = setupNs1();
        ns1.initialize();

        final OpenConnectionClient openConnectionClient = new OpenConnectionClient();
        openConnectionClient.openConnection(ns1Port, 1000, channelFuture -> {
            org.junit.Assert.assertTrue("Opening channel to port: " + ns1Port + " is a success", channelFuture.isSuccess());
            channelFuture.channel().close();
        });

        ns1.shutdown();
    }

    private static class DummyDiscardMessage implements MppRequestMessage
    {
        private static final long serialVersionUID = 1L;

        public boolean isResponseRequired()
        {
            return false;
        }

        public MppResponseMessage executeInLocalContext(NodeContext context)
        {
            return null;
        }
    }

    private static class DummyRequestMessage implements MppRequestMessage
    {
        private static final long serialVersionUID = 1L;

        public MppResponseMessage executeInLocalContext(NodeContext context)
        {
            return null;
        }
    }

    private static class ExpectingMessageHandler implements MppMessageHandler
    {

        List<MppRequestMessage> receivedMessages = new ArrayList<>();

        Consumer<MppRequestMessage> callback;

        CompletableFuture<Object> awaitMessageFuture = new CompletableFuture<>();

        public ExpectingMessageHandler(Consumer<MppRequestMessage> callback)
        {
            this.callback = callback;
        }

        public CompletableFuture<MppResponseMessage> handleMessage(MppRequestMessage requestMessage)
        {
            receivedMessages.add(requestMessage);
            callback.accept(requestMessage);
            awaitMessageFuture.complete(requestMessage);
            final CompletableFuture<MppResponseMessage> f = new CompletableFuture<>();
            f.complete(null);
            return f; // it never completes
        }

        public CompletableFuture<Object> getAwaitMessageFuture()
        {
            return awaitMessageFuture;
        }

        boolean receivedAnything()
        {
            return !receivedMessages.isEmpty();
        }
    }

    private static class ExpectingMessageHandlerWithResponse implements MppMessageHandler
    {

        interface ResponseCallback
        {
            MppResponseMessage accept(MppRequestMessage message);
        }

        List<MppRequestMessage> receivedMessages = new ArrayList<>();

        ResponseCallback callback;

        CompletableFuture<Object> awaitMessageFuture = new CompletableFuture<>();

        public ExpectingMessageHandlerWithResponse(ResponseCallback callback)
        {
            this.callback = callback;
        }

        public CompletableFuture<MppResponseMessage> handleMessage(MppRequestMessage requestMessage)
        {
            receivedMessages.add(requestMessage);
            final MppResponseMessage response = callback.accept(requestMessage);
            awaitMessageFuture.complete(requestMessage);
            final CompletableFuture<MppResponseMessage> f = new CompletableFuture<>();
            f.complete(response);
            return f;
        }

        public CompletableFuture<Object> getAwaitMessageFuture()
        {
            return awaitMessageFuture;
        }

        boolean receivedAnything()
        {
            return !receivedMessages.isEmpty();
        }
    }

    @Test
    public void testShouldSendDummyMessageThatGetsHandledWithoutResponse() throws UnknownHostException, InterruptedException, ExecutionException
    {
        CompletableFuture<Object> isTestDone = new CompletableFuture<>();
        final MppNetworkServiceImpl ns1 = setupNs1();
        final ExpectingMessageHandler ns1Executor = new ExpectingMessageHandler(x -> {
        });
        ns1.setMessageExecutor(ns1Executor);
        final ExpectingMessageHandler ns2Executor = new ExpectingMessageHandler(request -> {
            Assert.assertEquals("Message should be of type DummyDiscardMessage", DummyDiscardMessage.class, request.getClass());
            isTestDone.complete(null);
        });
        final MppNetworkService ns2 = setupNs2(ns2Executor);
        ns1.initialize();
        ns2.initialize();

        final DummyDiscardMessage dummyDiscardMessage = new DummyDiscardMessage();
        ns1.sendMessage(dummyDiscardMessage, NoMppMessageResponseExpectations.NO_MPP_MESSAGE_RESPONSE,
                        Arrays.asList(ns1.createReceipient(InetAddress.getLocalHost(), ns2Port)));


        isTestDone.get();
        Assert.assertTrue("NS2 has received something", ns2Executor.receivedAnything());
        Assert.assertFalse("NS1 has received nothing", ns1Executor.receivedAnything());

        try
        {
            ns1.shutdown();
            ns2.shutdown();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    private static class DummyResponse implements MppResponseMessage
    {
        final int payload;

        private DummyResponse(int payload)
        {
            this.payload = payload;
        }
    }

    @Test
    public void testShouldSendDummyRequestMessageAndThenReceiveSingleResponse() throws Exception
    {

        // ns2 sends REQUEST DummyRequestMessage to ns1 CHECK
        // ns1 sends RESPONSE DummyResponse with its port CHECK
        // ns1 sends RESPONSE back response to ns2
        // ns2 expects single RESPONSE.

        CompletableFuture<Object> isTestDone = new CompletableFuture<>();
        final MppNetworkServiceImpl ns1 = setupNs1();
        // ns1 produces DummyResponse with its port
        final ExpectingMessageHandlerWithResponse ns1MessageExecutor = new ExpectingMessageHandlerWithResponse(req -> new DummyResponse(ns1Port));
        ns1.setMessageExecutor(ns1MessageExecutor);

//        final ExpectingMessageExecutor ns2Executor = new ExpectingMessageExecutor(request -> {
//            Assert.assertEquals("Message should be of type DummyDiscardMessage", DummyResponse.class, request.getClass());
//            isTestDone.complete(null);
//        });
        final MppNetworkService ns2 = setupNs2();

        ns1.initialize();
        ns2.initialize();
        final DummyRequestMessage requestMessage = new DummyRequestMessage();
        // ns2 sends DummyRequestMessage to ns1
        final CompletableFuture<MppResponseMessage> responseF = ns2.sendMessage(requestMessage, new SingleMppMessageResponseExpectations(), Arrays.asList(ns1.createReceipient(InetAddress.getLocalHost(), ns1Port)));

        responseF.thenAccept(response -> {
            final DummyResponse dummyResponse = (DummyResponse) response;

            Assert.assertEquals("Is expected response", ns1Port, dummyResponse.payload);
            System.out.println("running expectations");
            isTestDone.complete(null);
        });

        isTestDone.get();
        ns1.shutdown();
        ns2.shutdown();
    }

    private class OpenConnectionClient
    {

        void openConnection(int portToConnect, long waitMillis, Consumer<ChannelFuture> doWithChannel) throws UnknownHostException, InterruptedException
        {
            EventLoopGroup workerGroup = new NioEventLoopGroup(1);

            try
            {
                Bootstrap b = new Bootstrap(); // (1)
                b.group(workerGroup); // (2)
                b.channel(NioSocketChannel.class); // (3)
                b.option(ChannelOption.SO_KEEPALIVE, true); // (4)
                b.handler(new ChannelInitializer<SocketChannel>()
                {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception
                    {

                    }
                });

                // Start the client.
                final ChannelFuture connect = b.connect(InetAddress.getLocalHost(), portToConnect);
                final boolean await = connect.await(waitMillis, TimeUnit.MILLISECONDS);// (5)
                doWithChannel.accept(connect);
                System.out.println("isSuccess" + connect.isSuccess());
                connect.channel().closeFuture().sync();
            }
            finally
            {
                workerGroup.shutdownGracefully();
            }
        }
    }

    private MppNetworkServiceImpl setupNs1()
    {
        final MppNetworkServiceImpl ns1 = new MppNetworkServiceImpl();

        ns1.setListeningPort(ns1Port);
        MppMessageHandler ns1Executor = new TestMessageHandler();
        ns1.setMessageExecutor(ns1Executor);
        return ns1;
    }

    private MppNetworkServiceImpl setupNs2()
    {
        MppMessageHandler ns2Executor = new TestMessageHandler();
        return setupNs2(ns2Executor);
    }

    private MppNetworkServiceImpl setupNs2(MppMessageHandler messageExecutor)
    {
        final MppNetworkServiceImpl ns2 = new MppNetworkServiceImpl();
        ns2.setListeningPort(ns2Port);
        ns2.setMessageExecutor(messageExecutor);
        return ns2;
    }
}
