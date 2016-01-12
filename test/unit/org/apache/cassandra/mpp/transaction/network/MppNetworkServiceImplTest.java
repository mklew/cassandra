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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.Test;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.cassandra.mpp.transaction.MppMessageExecutor;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 12/01/16
 */
public class MppNetworkServiceImplTest
{
    private static class TestMessageExecutor implements MppMessageExecutor {

        public CompletableFuture<MppResponseMessage> executeRequest(MppRequestMessage requestMessage)
        {
            return null;
        }
    }

    int ns1Port = 50001;

    int ns2Port = 50002;

    @Test
    public void testCreateMppNetworkService() {
        final MppNetworkServiceImpl ns1 = setupNs1();
        final MppNetworkServiceImpl ns2 = setupNs2();
        ns1.initialize();
        ns2.initialize();

        ns1.shutdown();
        ns2.shutdown();
    }

    @Test
    public void testShouldBeAbleToConnectAfterInitilized() throws InterruptedException, UnknownHostException
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

    private class OpenConnectionClient {

        void openConnection(int portToConnect, long waitMillis, Consumer<ChannelFuture> doWithChannel) throws UnknownHostException, InterruptedException
        {
            EventLoopGroup workerGroup = new NioEventLoopGroup(1);

            try {
                Bootstrap b = new Bootstrap(); // (1)
                b.group(workerGroup); // (2)
                b.channel(NioSocketChannel.class); // (3)
                b.option(ChannelOption.SO_KEEPALIVE, true); // (4)
                b.handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {

                    }
                });

                // Start the client.
                final ChannelFuture connect = b.connect(InetAddress.getLocalHost(), portToConnect);
                final boolean await = connect.await(waitMillis, TimeUnit.MILLISECONDS);// (5)
                doWithChannel.accept(connect);
                System.out.println("isSuccess" + connect.isSuccess());
                connect.channel().closeFuture().sync();

            } finally {
                workerGroup.shutdownGracefully();
            }
        }

    }

    private MppNetworkServiceImpl setupNs1()
    {
        final MppNetworkServiceImpl ns1 = new MppNetworkServiceImpl();

        ns1.setListeningPort(ns1Port);
        MppMessageExecutor ns1Executor = new TestMessageExecutor();
        ns1.setMessageExecutor(ns1Executor);
        return ns1;
    }

    private MppNetworkServiceImpl setupNs2()
    {
        final MppNetworkServiceImpl ns2 = new MppNetworkServiceImpl();

        ns2.setListeningPort(ns2Port);
        MppMessageExecutor ns2Executor = new TestMessageExecutor();
        ns2.setMessageExecutor(ns2Executor);
        return ns2;
    }
}
