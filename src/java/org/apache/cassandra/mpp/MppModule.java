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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.mpp.transaction.MppService;
import org.apache.cassandra.mpp.transaction.internal.MppServiceImpl;
import org.apache.cassandra.mpp.transaction.internal.PrivateMemtableStorageImpl;
import org.apache.cassandra.mpp.transaction.internal.ReadTransactionDataServiceImpl;
import org.apache.cassandra.mpp.transaction.network.MppNetworkService;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 21/01/16
 */
public class MppModule
{
    private MppService mppService;

    private static final Logger logger = LoggerFactory.getLogger(MppModule.class);

    public MppModule(MppService service)
    {
        this.mppService = service;
    }

    public static MppModule createModule(MppNetworkService mppNetworkService)
    {
        logger.info("MppModule is being created");
        final MppServiceImpl service = new MppServiceImpl();
        final PrivateMemtableStorageImpl privateMemtableStorage = new PrivateMemtableStorageImpl();

        ReadTransactionDataServiceImpl readTransactionDataService = new ReadTransactionDataServiceImpl();
        service.setPrivateMemtableStorage(privateMemtableStorage);
        readTransactionDataService.setMppNetworkService(mppNetworkService);

        return new MppModule(service);
    }

    public MppService getMppService()
    {
        return mppService;
    }
}
