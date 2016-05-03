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

package org.apache.cassandra.mpp;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.mpp.transaction.MppIndexKeys;
import org.apache.cassandra.mpp.transaction.MppService;
import org.apache.cassandra.mpp.transaction.MppTransactionLog;
import org.apache.cassandra.mpp.transaction.internal.MpPaxosIndex;
import org.apache.cassandra.mpp.transaction.internal.MppIndexKeysImpl;
import org.apache.cassandra.mpp.transaction.internal.MppServiceImpl;
import org.apache.cassandra.mpp.transaction.internal.MppTransactionLogImpl;
import org.apache.cassandra.mpp.transaction.internal.NativeReadTransactionDataRequestExecutor;
import org.apache.cassandra.mpp.transaction.internal.PrivateMemtableStorageImpl;
import org.apache.cassandra.mpp.transaction.internal.ReadTransactionDataServiceImpl;
import org.apache.cassandra.mpp.transaction.network.MppNetworkService;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageProxy;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 21/01/16
 */
public class MppModule
{
    private final ReadTransactionDataServiceImpl readTransactionDataService;
    private final MppService mppService;
    private final MppTransactionLog transactionLog;

    private static final Logger logger = LoggerFactory.getLogger(MppModule.class);

    public MppModule(MppService service, ReadTransactionDataServiceImpl readTransactionDataService, MppTransactionLog transactionLog)
    {
        this.mppService = service;
        this.readTransactionDataService = readTransactionDataService;
        this.transactionLog = transactionLog;
    }

    public static MppModule createModule(MppNetworkService mppNetworkService)
    {
        logger.info("MppModule is being created");
        MppIndexKeys mppIndexKeys = new MppIndexKeysImpl();
        final MppServiceImpl service = new MppServiceImpl();
        final PrivateMemtableStorageImpl privateMemtableStorage = new PrivateMemtableStorageImpl();
        MpPaxosIndex mpPaxosIndex = new MpPaxosIndex();

        MppTransactionLog transactionLog = new MppTransactionLogImpl();

        mpPaxosIndex.setDeleteTransactionsDataService(privateMemtableStorage);
        mpPaxosIndex.setJmxRolledBackTxsInfo(service);
        mpPaxosIndex.setMppIndexKeys(mppIndexKeys);

        final NativeReadTransactionDataRequestExecutor nativeReadTransactionDataRequestExecutor = new NativeReadTransactionDataRequestExecutor(MessagingService.instance());

        ReadTransactionDataServiceImpl readTransactionDataService = new ReadTransactionDataServiceImpl();
        readTransactionDataService.setExecutor(nativeReadTransactionDataRequestExecutor);

        service.setPrivateMemtableStorage(privateMemtableStorage);
        service.setReadTransactionDataService(readTransactionDataService);
        service.setMpPaxosIndex(mpPaxosIndex);
        service.setMessagingService(MessagingService.instance());
        service.setHintsService((mutation, destination) -> StorageProxy.submitHint(mutation, destination, null));

        return new MppModule(service, readTransactionDataService, transactionLog);
    }

    public MppService getMppService()
    {
        return mppService;
    }

    public MppTransactionLog getTransactionLog()
    {
        return transactionLog;
    }

    @VisibleForTesting
    public ReadTransactionDataServiceImpl getReadTransactionDataServiceImpl()
    {
        return readTransactionDataService;
    }
}
