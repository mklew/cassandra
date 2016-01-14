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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
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
    private static class NoOpMessageHandler implements MppMessageHandler
    {

        public CompletableFuture<MppResponseMessage> handleMessage(MppRequestMessage requestMessage)
        {
            return null;
        }
    }

    int ns1Port = 50001;

    int ns2Port = 50002;

    private static class NsServiceRef
    {

        private static final int PORT_BASE = 50_000;

        private final String name;

        private final int id;

        private MppNetworkServiceImpl mppNetworkService;

        private MppMessageHandler handler = new NoOpMessageHandler();

        private NsServiceRef(String name, int id)
        {
            this.name = name;
            this.id = id;
        }

        String getNsServiceName()
        {
            return name;
        }

        Integer getId()
        {
            return id;
        }

        int getPort()
        {
            return PORT_BASE + (id * 100);
        }

        public Void init()
        {
            Preconditions.checkArgument(mppNetworkService == null);
            mppNetworkService = new MppNetworkServiceImpl();
            mppNetworkService.setLimitNumberOfEventLoopThreads(2);
            mppNetworkService.setMessageHandler(handler);
            mppNetworkService.setListeningPort(getPort());

            mppNetworkService.initialize();

            return null;
        }

        public void setMessageHandler(MppMessageHandler handler)
        {
            this.handler = handler;
            if (mppNetworkService != null)
            {
                mppNetworkService.setMessageHandler(handler);
            }
        }

        public Void shutdown()
        {
            try
            {
                mppNetworkService.shutdown();
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
            return null;
        }

        public <T> CompletableFuture<T> sendMessage(MppMessage message, MppMessageResponseExpectations<T> mppMessageResponseExpectations, Collection<NsServiceRef> receipientRefs)
        {
            final List<MppNetworkService.MessageReceipient> messageReceipients = receipientRefs.stream().map(r -> {
                try
                {
                    return mppNetworkService.createReceipient(InetAddress.getLocalHost(), r.getPort());
                }
                catch (UnknownHostException e)
                {
                    throw new RuntimeException(e);
                }
            }).collect(Collectors.toList());

            return mppNetworkService.sendMessage(message, mppMessageResponseExpectations, messageReceipients);
        }
    }

    private interface NsServiceLookup
    {
        NsServiceRef getById(int id);

        NsServiceRef getByName(String name);
    }

    private static class NsServiceProducer implements NsServiceLookup
    {

        AtomicInteger nsServiceId = new AtomicInteger(1);

        Map<String, NsServiceRef> nameToNsServiceRef = new HashMap<>();

        public NsServiceRef createNextNsService()
        {
            return createNextNsService("");
        }

        public NsServiceRef createNextNsService(String givenName)
        {
            final int nsId = nsServiceId.getAndIncrement();
            String nsName = "NS#" + nsId + "[" + givenName + "]";
            final NsServiceRef nsServiceRef = new NsServiceRef(nsName, nsId);

            nameToNsServiceRef.put(givenName, nsServiceRef);
            return nsServiceRef;
        }

        public void initServices()
        {
            nameToNsServiceRef.entrySet().stream().forEach(e -> e.getValue().init());
        }

        public void shutdownServices()
        {
            nameToNsServiceRef.entrySet().stream().forEach(e -> e.getValue().shutdown());
        }

        public NsServiceRef getById(int id)
        {
            return nameToNsServiceRef.entrySet().stream().filter(r -> r.getValue().getId().equals(id)).findFirst().get().getValue();
        }

        public NsServiceRef getByName(String name)
        {
            final NsServiceRef nsServiceRef = nameToNsServiceRef.get(name);
            Preconditions.checkArgument(nsServiceRef != null);
            return nsServiceRef;
        }
    }

    private abstract static class TestWithNsServices
    {

        final NsServiceProducer nsServiceProducer = getNsServiceProducer();

        void run() throws Exception
        {
            setup(nsServiceProducer);
            nsServiceProducer.initServices();
            try
            {
                runTest(nsServiceProducer);
            }
            finally
            {
                nsServiceProducer.shutdownServices();
            }
        }

        protected <T> CompletableFuture<T> sendMessage(String from,
                                                      MppMessage message,
                                                      MppMessageResponseExpectations<T> mppMessageResponseExpectations,
                                                      String ... receipients) {
            final NsServiceRef ref = nsServiceProducer.getByName(from);

            Collection<NsServiceRef> receipientRefs = new ArrayList<>();
            for (String receipient : receipients)
            {
                receipientRefs.add(nsServiceProducer.getByName(receipient));
            }

            return ref.sendMessage(message, mppMessageResponseExpectations, receipientRefs);
        }


        abstract protected void setup(NsServiceProducer nsServiceProducer);

        abstract protected void runTest(NsServiceLookup nsServiceLookup) throws Exception;
    }

    private static NsServiceProducer getNsServiceProducer()
    {
        return new NsServiceProducer();
    }

    @Test
    public void testCreateMppNetworkServicesWithTester() throws Exception
    {
        new TestWithNsServices()
        {
            protected void setup(NsServiceProducer nsServiceProducer)
            {
                final NsServiceRef ns1 = nsServiceProducer.createNextNsService();
                final NsServiceRef ns2 = nsServiceProducer.createNextNsService("second");
            }

            protected void runTest(NsServiceLookup nsServiceLookup)
            {
                // do nothing
            }
        }.run();
    }

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

    @Test
    public void testShouldBeAbleToConnectAfterInitilizedWithTester() throws Exception
    {
        new TestWithNsServices()
        {
            protected void setup(NsServiceProducer nsServiceProducer)
            {
                nsServiceProducer.createNextNsService();
            }

            protected void runTest(NsServiceLookup nsServiceLookup) throws Exception
            {
                final OpenConnectionClient openConnectionClient = new OpenConnectionClient();
                final int portForNs1 = nsServiceLookup.getById(1).getPort();
                openConnectionClient.openConnection(portForNs1, 1000, channelFuture -> {
                    org.junit.Assert.assertTrue("Opening channel to port: " + portForNs1 + " is a success", channelFuture.isSuccess());
                    channelFuture.channel().close();
                });
            }
        }.run();
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
    public void testShouldSendDummyMessageThatGetsHandledWithoutResponseWithTester() throws Exception {
        CompletableFuture<Object> isTestDone = new CompletableFuture<>();

        final ExpectingMessageHandler ns1Executor = new ExpectingMessageHandler(x -> {
        });

        final ExpectingMessageHandler ns2Executor = new ExpectingMessageHandler(request -> {
            Assert.assertEquals("Message should be of type DummyDiscardMessage", DummyDiscardMessage.class, request.getClass());
            isTestDone.complete(null);
        });

        new TestWithNsServices()
        {
            protected void setup(NsServiceProducer nsServiceProducer)
            {
                final NsServiceRef node1 = nsServiceProducer.createNextNsService("node1");
                node1.setMessageHandler(ns1Executor);
                final NsServiceRef node2 = nsServiceProducer.createNextNsService("node2");
                node2.setMessageHandler(ns2Executor);
            }

            protected void runTest(NsServiceLookup nsServiceLookup) throws Exception
            {
                final DummyDiscardMessage dummyDiscardMessage = new DummyDiscardMessage();
                sendMessage("node1", dummyDiscardMessage, NoMppMessageResponseExpectations.NO_MPP_MESSAGE_RESPONSE, "node2");
            }
        }.run();
        isTestDone.get();
        Assert.assertTrue("NS2 has received something", ns2Executor.receivedAnything());
        Assert.assertFalse("NS1 has received nothing", ns1Executor.receivedAnything());

    }

    private static class RequestQuorumMessage implements MppRequestMessage {

        public MppResponseMessage executeInLocalContext(NodeContext context)
        {
            return null;
        }
    }

    private static class TestQuorumMessageResponse implements MppResponseMessage {
        private final int value;

        private TestQuorumMessageResponse(int value)
        {
            this.value = value;
        }
    }

    @Test
    public void testQuorumValue() throws Exception
    {
        final int expectedValue = 132;
        final ExpectingMessageHandlerWithResponse n1Handler = new ExpectingMessageHandlerWithResponse(req -> new TestQuorumMessageResponse(expectedValue));
        final ExpectingMessageHandlerWithResponse n2Handler = new ExpectingMessageHandlerWithResponse(req -> new TestQuorumMessageResponse(expectedValue));
        final ExpectingMessageHandlerWithResponse n3Handler = new ExpectingMessageHandlerWithResponse(req -> new TestQuorumMessageResponse(expectedValue - 1));
        final ExpectingMessageHandlerWithResponse n4Handler = new ExpectingMessageHandlerWithResponse(req -> new TestQuorumMessageResponse(expectedValue));

        CompletableFuture<Object> isTestDone = new CompletableFuture<>();
        new TestWithNsServices()
        {
            protected void setup(NsServiceProducer nsServiceProducer)
            {
                nsServiceProducer.createNextNsService("n1").setMessageHandler(n1Handler);
                nsServiceProducer.createNextNsService("n2").setMessageHandler(n2Handler);
                nsServiceProducer.createNextNsService("n3").setMessageHandler(n3Handler);
                nsServiceProducer.createNextNsService("n4").setMessageHandler(n4Handler);
            }

            protected void runTest(NsServiceLookup nsServiceLookup) throws Exception
            {
                final CompletableFuture<Collection<MppResponseMessage>> responses = sendMessage("n1", new RequestQuorumMessage(), new QuorumMppMessageResponseExpectations(4), "n2", "n3", "n4");

                responses.thenAccept(rs -> {
                    final Map<Integer, Long> valueToCount = rs.stream()
                                                         .map(x -> (TestQuorumMessageResponse) x)
                                                         .map(x -> x.value)
                                                         .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

                    final Integer quorumReadValue = valueToCount.entrySet().stream().reduce((op1, op2) -> {
                        if (op1.getValue() > op2.getValue()) return op1;
                        else return op2;
                    }).get().getKey();

                    Assert.assertEquals(expectedValue, quorumReadValue.intValue());
                    isTestDone.complete(null);
                });
            }
        }.run();

        isTestDone.get();

    }


    // TODO [MPP] Tests that take into account:
    // TODO - timeout of an request
    // TODO - node that can be connected to, but it does not answer with response
    // TODO - node that cannot be connected to (it is down)


    @Test
    public void testShouldSendDummyMessageThatGetsHandledWithoutResponse() throws UnknownHostException, InterruptedException, ExecutionException
    {
        CompletableFuture<Object> isTestDone = new CompletableFuture<>();
        final MppNetworkServiceImpl ns1 = setupNs1();
        final ExpectingMessageHandler ns1Executor = new ExpectingMessageHandler(x -> {
        });
        ns1.setMessageHandler(ns1Executor);
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
        ns1.setMessageHandler(ns1MessageExecutor);

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
        MppMessageHandler ns1Handler = new NoOpMessageHandler();
        ns1.setMessageHandler(ns1Handler);
        return ns1;
    }

    private MppNetworkServiceImpl setupNs2()
    {
        MppMessageHandler ns2Executor = new NoOpMessageHandler();
        return setupNs2(ns2Executor);
    }

    private MppNetworkServiceImpl setupNs2(MppMessageHandler messageExecutor)
    {
        final MppNetworkServiceImpl ns2 = new MppNetworkServiceImpl();
        ns2.setListeningPort(ns2Port);
        ns2.setMessageHandler(messageExecutor);
        return ns2;
    }
}
