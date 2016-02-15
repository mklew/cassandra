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

package org.apache.cassandra.mpp.transaction;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;

/**
 *
 * Stores data to private memtables {@link PrivateMemtable}
 *
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 01/11/15
 */
public interface PrivateMemtableStorage extends DeleteTransactionsDataService
{
    /**
     * Stores mutation in PrivateMemtable for this transaction in its TransactionData
     *
     * If no TransactionsData existed for this transaction then it is created, else updated.
     *
     * @param txId transaction id
     * @param mutation to store
     */
    void storeMutation(TransactionId txId, Mutation mutation);


    /**
     * For tests.
     * Dump all information stored for this transaction.
     *
     * @param txId transaction id
     * @return transactionData for this txId if it exists or empty transaction data.
     */
    TransactionData readTransactionData(TransactionId txId);

    Map<TransactionItem, Optional<PartitionUpdate>> readTransactionItems(TransactionId transactionId, List<TransactionItem> transactionItems);

    default Map<TransactionItem, Optional<PartitionUpdate>> readTransactionItems(UUID transactionId, List<TransactionItem> transactionItems) {
        return readTransactionItems(new TransactionTimeUUID(transactionId), transactionItems);
    }

    boolean transactionExistsInStorage(TransactionId transactionId);


    void removePrivateData(TransactionId id);

    Collection<TransactionId> getInProgressTransactions();
}
