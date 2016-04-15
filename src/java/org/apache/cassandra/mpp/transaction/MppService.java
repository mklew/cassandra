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
import java.util.Optional;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.TransactionalMutation;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.mpp.transaction.internal.JmxRolledBackTxsInfo;
import org.apache.cassandra.mpp.transaction.internal.MppHint;
import org.apache.cassandra.mpp.transaction.paxos.MpPaxosId;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MppServiceMXBean;
import org.apache.cassandra.service.mppaxos.MpPrePrepare;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 20/01/16
 */
public interface MppService extends MppServiceMXBean, MultiPartitionPaxosIndex, JmxRolledBackTxsInfo
{
    String MBEAN_NAME = "org.apache.cassandra.mpp.transaction:type=MppService";

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
     * @param transactionState
     * @param consistencyLevel
     * @param options
     * @param clientState
     */
    boolean commitTransaction(TransactionState transactionState, ConsistencyLevel consistencyLevel, QueryOptions options, ClientState clientState);

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

    Optional<PartitionUpdate> readSingleTransactionItem(TransactionState transactionState);

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

    void readAllByColumnFamily(TransactionId transactionId, String ksName, String cfName, Consumer<PartitionIterator> cb);

    void readAllByColumnFamilyAndToken(TransactionId transactionId, String ksName, String cfName, Token token, Consumer<PartitionIterator> cb);

    void readQuorumByColumnFamily(TransactionState transactionState, String ksName, String cfNameColumnFamily, ConsistencyLevel consistencyLevel, Consumer<PartitionIterator> consumer);

    void readQuorumByColumnFamilyAndToken(TransactionState transactionState, String ksName, String cfNameColumnFamily, Token token, ConsistencyLevel consistency, Consumer<PartitionIterator> consumer);

    void flushTransactionLocally(TransactionId transactionId);

    void makeTransactionDataConsistent(TransactionState transactionState);

    Optional<MpPaxosId> prePrepareMultiPartitionPaxos(MpPrePrepare prePrepare);

    void multiPartitionPaxosCommitPhase(TransactionState transactionState, long ballot);

    void submitHints(List<MppHint> hints);

    void transactionalRead(TransactionState transactionState, ConsistencyLevel consistencyLevel, ClientState state);
}
