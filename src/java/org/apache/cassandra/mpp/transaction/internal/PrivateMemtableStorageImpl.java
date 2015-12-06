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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.mpp.transaction.PrivateMemtableStorage;
import org.apache.cassandra.mpp.transaction.TransactionData;
import org.apache.cassandra.mpp.transaction.TransactionId;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;
import org.apache.cassandra.utils.Pair;

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

    // TODO [MPP] I need TTL for private memtables. Something already exists in cassadra, prepared statements maybe?
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

    public Map<TransactionItem, List<PartitionUpdate>> readTransactionItems(TransactionId transactionId, List<TransactionItem> transactionItems)
    {
        final TransactionData transactionData = readTransactionData(transactionId);
        final Stream<Pair<TransactionItem, List<PartitionUpdate>>> pairStream = transactionItems.stream().map(readTransaction(transactionData));

        return pairStream.collect(Collectors.toMap(p -> p.left, v -> v.right));
    }

    private static Function<TransactionItem, Pair<TransactionItem, List<PartitionUpdate>>> readTransaction(TransactionData transactionData)
    {
        return item -> {
            final List<PartitionUpdate> partitionUpdates = transactionData.readData(item.getKsName(),
                                                                                    findColumnFamilyId(item),
                                                                                    item.getToken())
                                                                          .collect(Collectors.toList());
            return Pair.create(item, partitionUpdates);
        };
    }

    private static UUID findColumnFamilyId(TransactionItem item)
    {
        return Schema.instance.getId(item.getKsName(), item.getCfName());
    }
}
