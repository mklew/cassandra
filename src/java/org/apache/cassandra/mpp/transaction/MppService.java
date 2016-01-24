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

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.TransactionalMutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;
import org.apache.cassandra.mpp.transaction.client.TransactionState;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 20/01/16
 */
public interface MppService
{
    /**
     * CQL: START TRANSACTION
     *
     * Returns TransactionState with transaction_id and empty list of transaction items.
     * Doesn't have to initiate nothing in private memtable storage because that happens with first operation.
     *
     */
    TransactionState startTransaction();

    /**
     * This has to start paxos rounds.
     *
     * TODO [MPP] Test it via my networking
     */
    void commitTransaction();

    /**
     * This has to message all nodes that took part in transaction and tell them to get rid of private memtables
     * for that transaction
     *
     * TODO [MPP] Test it via my networking
     */
    void rollbackTransaction(TransactionState transactionState);

    /**
     * @param transactionState may contain empty list of transaction items because it is only relevant to this node
     */
    void rollbackTransactionLocal(TransactionState transactionState);

    /**
     * Reads transaction data's just, but just on this node.
     *
     * @param transactionId
     * @return
     */
    Map<TransactionItem, List<PartitionUpdate>> readTransactionDataLocalOnly(TransactionId transactionId);

    /**
     * Reads all data for that transaction, it forwards reads to other nodes in that transaction.
     *
     *
     * @param transactionState
     * @return
     */
    Map<TransactionItem, List<PartitionUpdate>> readAllTransactionData(TransactionState transactionState);

    /**
     * In progress transactions at this node.
     *
     * @return
     */
    Collection<TransactionId> getInProgressTransactions();

    /**
     * TODO [MPP] Test it via my networking
     */
    TransactionItem executeTransactionalMutationLocally(TransactionalMutation transactionalMutation);

    /**
     * Returns just transaction item without executing that mutation
     */
    TransactionItem getTransactionItemForMutationNoExecution(TransactionalMutation transactionalMutation);

    @VisibleForTesting
    boolean transactionExistsOnThisNode(TransactionId transactionId);

    TransactionState readLocalTransactionState(TransactionId transactionId);
}
