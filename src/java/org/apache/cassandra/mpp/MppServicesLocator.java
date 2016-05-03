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

import com.google.common.base.Preconditions;

import org.apache.cassandra.mpp.transaction.MppService;
import org.apache.cassandra.mpp.transaction.MppTransactionLog;
import org.apache.cassandra.mpp.transaction.MultiPartitionPaxosIndex;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 21/01/16
 */
public class MppServicesLocator
{
    private static MppService mppService;

    private static MppTransactionLog transactionLog;

    private MppServicesLocator()
    {
    }

    public static MppService getInstance()
    {
        Preconditions.checkNotNull(mppService, "MppServiceLocator has not been initialized");
        return mppService;
    }

    public static MultiPartitionPaxosIndex getIndexInstance()
    {
        return getInstance();
    }

    public static void setInstance(MppService mppService)
    {
        MppServicesLocator.mppService = mppService;
    }

    public static void setTransactionLog(MppTransactionLog transactionLog)
    {
        MppServicesLocator.transactionLog = transactionLog;
    }

    public static MppTransactionLog getTransactionLog()
    {
        Preconditions.checkNotNull(transactionLog, "MppServiceLocator has not been initialized with transactionLog");
        return transactionLog;
    }
}
