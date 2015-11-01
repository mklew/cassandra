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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.mpp.transaction.PrivateMemtableStorage;
import org.apache.cassandra.mpp.transaction.TransactionData;
import org.apache.cassandra.mpp.transaction.TransactionId;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 01/11/15
 */
public class PrivateMemtableStorageImpl implements PrivateMemtableStorage
{
    private final ConcurrentMap<TransactionId, TransactionData> txIdToData = new ConcurrentHashMap<>();

    static TransactionData createNewTransactionData(TransactionId txId)
    {
        return new TransactionDataImpl(txId);
    }

    public void storeMutation(TransactionId txId, Mutation mutation)
    {
        final TransactionData transactionData = txIdToData.computeIfAbsent(txId, PrivateMemtableStorageImpl::createNewTransactionData);
        transactionData.addMutation(mutation);
    }

    public TransactionData readTransactionData(TransactionId txId)
    {
        final TransactionData transactionData = txIdToData.get(txId);
        if(transactionData != null) {
            return transactionData;
        }
        else
        {
            return new EmptyTransactionData(txId);
        }
    }
}
