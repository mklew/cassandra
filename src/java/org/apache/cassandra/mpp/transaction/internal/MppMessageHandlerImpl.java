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

package org.apache.cassandra.mpp.transaction.internal;

import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.mpp.transaction.MppMessageHandler;
import org.apache.cassandra.mpp.transaction.MppService;
import org.apache.cassandra.mpp.transaction.NodeContext;
import org.apache.cassandra.mpp.transaction.PrivateMemtableStorage;
import org.apache.cassandra.mpp.transaction.ReadTransactionDataService;
import org.apache.cassandra.mpp.transaction.network.MppRequestMessage;
import org.apache.cassandra.mpp.transaction.network.MppResponseMessage;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 06/12/15
 */
public class MppMessageHandlerImpl implements MppMessageHandler
{

    private static final Logger logger = LoggerFactory.getLogger(MppMessageHandlerImpl.class);

    private PrivateMemtableStorage privateMemtableStorage;

    private ReadTransactionDataService readTransactionDataService;

    private MppService mppService;


    public CompletableFuture<MppResponseMessage> handleMessage(MppRequestMessage requestMessage)
    {
        final CompletableFuture<MppResponseMessage> f = new CompletableFuture<>();
        final MppResponseMessage mppResponseMessage = requestMessage.executeInLocalContext(createNodeContext());
        f.complete(mppResponseMessage);
        return f;
    }

    private NodeContext createNodeContext()
    {
        return new NodeContext()
        {
            public PrivateMemtableStorage getStorage()
            {
                return getPrivateMemtableStorage();
            }

            public ReadTransactionDataService readService()
            {
                return getReadTransactionDataService();
            }

            public MppService getService() { return getMppService(); }
        };
    }

    public MppService getMppService()
    {
        return mppService;
    }

    public void setMppService(MppService mppService)
    {
        this.mppService = mppService;
    }

    public PrivateMemtableStorage getPrivateMemtableStorage()
    {
        return privateMemtableStorage;
    }

    public void setPrivateMemtableStorage(PrivateMemtableStorage privateMemtableStorage)
    {
        this.privateMemtableStorage = privateMemtableStorage;
    }

    public ReadTransactionDataService getReadTransactionDataService()
    {
        return readTransactionDataService;
    }

    public void setReadTransactionDataService(ReadTransactionDataService readTransactionDataService)
    {
        this.readTransactionDataService = readTransactionDataService;
    }
}
