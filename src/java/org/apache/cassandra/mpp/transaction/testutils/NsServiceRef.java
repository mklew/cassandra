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

package org.apache.cassandra.mpp.transaction.testutils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.cassandra.mpp.transaction.MppMessageHandler;
import org.apache.cassandra.mpp.transaction.internal.NoOpMessageHandler;
import org.apache.cassandra.mpp.transaction.network.MessageResult;
import org.apache.cassandra.mpp.transaction.network.MppMessage;
import org.apache.cassandra.mpp.transaction.network.MppMessageReceipient;
import org.apache.cassandra.mpp.transaction.network.MppMessageResponseExpectations;
import org.apache.cassandra.mpp.transaction.network.MppNetworkHooks;
import org.apache.cassandra.mpp.transaction.network.MppNetworkService;
import org.apache.cassandra.mpp.transaction.network.MppNetworkServiceImpl;

/**
* @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
* @since 20/01/16
*/
public class NsServiceRef implements NsServicePortRef
{

    private static final int PORT_BASE = 50_000;

    private final String name;

    private final int id;

    private MppNetworkServiceImpl mppNetworkService = new MppNetworkServiceImpl();

    private MppMessageHandler handler = new NoOpMessageHandler();

    public NsServiceRef(String name, int id)
    {
        this.name = name;
        this.id = id;
    }

    public String getNsServiceName()
    {
        return name;
    }

    public Integer getId()
    {
        return id;
    }

    public int getPort()
    {
        return PORT_BASE + (id * 100);
    }

    public Void init()
    {
//            Preconditions.checkArgument(mppNetworkService == null);
//            mppNetworkService = new MppNetworkServiceImpl();
        mppNetworkService.setLimitNumberOfEventLoopThreads(2);
        mppNetworkService.setMessageHandler(handler);
        mppNetworkService.setListeningPort(getPort());
        mppNetworkService.setName(getNsServiceName());

        mppNetworkService.start();

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

    public <T> MessageResult<T> sendMessage(MppMessage message, MppMessageResponseExpectations<T> mppMessageResponseExpectations, Collection<NsServicePortRef> receipientRefs)
    {
        final List<MppMessageReceipient> messageReceipients = receipientRefs.stream().map(r -> {
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


    public NsServiceRef setDefaultTimeout(long responseTimeout)
    {
        mppNetworkService.setDefaultTimeout(responseTimeout);
        return this;
    }


    public NsServiceRef setHooks(MppNetworkHooks hooks)
    {
        mppNetworkService.setHooks(hooks);
        return this;
    }

    public MppNetworkService getMppNetworkService()
    {
        return mppNetworkService;
    }

    public NsServiceRef setTimeoutHandlingEnabled(boolean enabled)
    {
        mppNetworkService.setTimeoutHandlingEnabled(enabled);
        return this;
    }
}
