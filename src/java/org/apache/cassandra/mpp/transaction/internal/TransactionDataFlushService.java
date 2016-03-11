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

import org.apache.cassandra.mpp.transaction.PrivateMemtableStorage;
import org.apache.cassandra.mpp.transaction.ReadTransactionDataService;
import org.apache.cassandra.mpp.transaction.TransactionData;
import org.apache.cassandra.mpp.transaction.TransactionId;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 29/11/15
 */
public class TransactionDataFlushService
{

    private final ReadTransactionDataService readTransactionDataService;

    public TransactionDataFlushService(ReadTransactionDataService readTransactionDataService)
    {
        this.readTransactionDataService = readTransactionDataService;
    }

    /**
     * TODO [MPP] Implement that
     *
     * @param transactionData
     * @return {@code true} if changes were applied. {@code false} if nothing happened
     */
    public boolean flushTransactionData(TransactionData transactionData) {
//        if(transactionData == null) {
//            return false;
//        }
//        else {
//            transactionData.
//        }
        throw new NotImplementedException();
    }


    public boolean flushTransactionData(TransactionId transactionId, PrivateMemtableStorage storage) {
        final TransactionData transactionData = storage.readTransactionData(transactionId);
        return flushTransactionData(transactionData);
    }
}
