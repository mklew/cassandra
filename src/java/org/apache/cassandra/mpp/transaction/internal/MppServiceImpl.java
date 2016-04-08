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

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.Json;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.TransactionalMutation;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterators;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.TransactionRolledBackException;
import org.apache.cassandra.mpp.transaction.HintsService;
import org.apache.cassandra.mpp.transaction.MppService;
import org.apache.cassandra.mpp.transaction.PrivateMemtableStorage;
import org.apache.cassandra.mpp.transaction.ReadTransactionDataService;
import org.apache.cassandra.mpp.transaction.TransactionData;
import org.apache.cassandra.mpp.transaction.TransactionId;
import org.apache.cassandra.mpp.transaction.TransactionTimeUUID;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.mpp.transaction.client.TransactionStateUtils;
import org.apache.cassandra.mpp.transaction.client.dto.TransactionItemDto;
import org.apache.cassandra.mpp.transaction.client.dto.TransactionStateDto;
import org.apache.cassandra.mpp.transaction.paxos.MpPaxosId;
import org.apache.cassandra.mpp.transaction.serialization.TransactionStateSerializer;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.mppaxos.MpPrePrepare;
import org.apache.cassandra.service.mppaxos.MpPrePrepareMpPaxosCallback;
import org.apache.cassandra.service.mppaxos.ReplicasGroupsOperationCallback;
import org.apache.cassandra.utils.FBUtilities;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 20/01/16
 */
public class MppServiceImpl implements MppService
{
    private static final Logger logger = LoggerFactory.getLogger(MppServiceImpl.class);

    private PrivateMemtableStorage privateMemtableStorage;

    private ReadTransactionDataService readTransactionDataService;

    private MpPaxosIndex mpPaxosIndex;

    private MessagingService messagingService;

    private HintsService hintsService;

    public void setHintsService(HintsService hintsService)
    {
        this.hintsService = hintsService;
    }

    public MessagingService getMessagingService()
    {
        return messagingService;
    }

    public void setMessagingService(MessagingService messagingService)
    {
        this.messagingService = messagingService;
    }

    public MpPaxosIndex getMpPaxosIndex()
    {
        return mpPaxosIndex;
    }

    public void setMpPaxosIndex(MpPaxosIndex mpPaxosIndex)
    {
        this.mpPaxosIndex = mpPaxosIndex;
    }

    public ReadTransactionDataService getReadTransactionDataService()
    {
        return readTransactionDataService;
    }

    public void setReadTransactionDataService(ReadTransactionDataService readTransactionDataService)
    {
        this.readTransactionDataService = readTransactionDataService;
    }

    public PrivateMemtableStorage getPrivateMemtableStorage()
    {
        return privateMemtableStorage;
    }

    public void setPrivateMemtableStorage(PrivateMemtableStorage privateMemtableStorage)
    {
        this.privateMemtableStorage = privateMemtableStorage;
    }

    public TransactionState startTransaction()
    {
        final UUID txId = UUIDs.timeBased();
        logger.info("Begin transaction id {}", txId);
        return new TransactionState(txId, Collections.emptyList());
    }

    public static ConsistencyLevel MPP_HARDCODED_CONSISTENCY_LEVEL = ConsistencyLevel.LOCAL_TRANSACTIONAL;

    /**
     * It should block until end of transaction, and then return.
     *  @param transactionState
     * @param consistencyLevel
     * @param options
     * @param clientState
     */
    public void commitTransaction(TransactionState transactionState, ConsistencyLevel consistencyLevel, QueryOptions options, ClientState clientState)
    {
        logger.info("Commit transaction called with transaction state {} and consistency level {}", transactionState, consistencyLevel);

        List<ReplicasGroupAndOwnedItems> replicasAndOwnedItems = ForEachReplicaGroupOperations.groupItemsByReplicas(transactionState);
        StorageProxyMpPaxosExtensions.ReplicaGroupsPhaseExecutor multiPartitionPaxosPhaseExecutor = StorageProxyMpPaxosExtensions.createMultiPartitionPaxosPhaseExecutor(replicasAndOwnedItems, transactionState, clientState);

        try
        {
            multiPartitionPaxosPhaseExecutor.tryToExecute();
        }
        catch (TransactionRolledBackException e)
        {
            // TODO [MPP] Commenting it out, because it will get rolled back by other transaction.
//            rollbackTransactionInternal(e.getRolledBackTransaction(), replicasAndOwnedItems);
            throw e;
        }

//        ReplicasGroupsOperationCallback replicasOperationsCallback = new ReplicasGroupsOperationCallback(replicasAndOwnedItems);
//        List<MpPrePrepareMpPaxosCallback> callbacksForPhase1 = replicasAndOwnedItems.stream().parallel().map(replicaGroupWithItems -> {
//
//            return prePrepareReplicaGroup(transactionState, replicaGroupWithItems, replicasOperationsCallback);
//        }).collect(Collectors.toList());
//
//        callbacksForPhase1.forEach(callbackForPrePrepare -> {
//            callbackForPrePrepare.await();
//
//            callbackForPrePrepare.getResponsesByReplica().entrySet().stream().forEach(kv -> {
//                logger.debug("Replica {} has pre preared paxos id {}", kv.getKey(), kv.getValue().map(id -> id.getPaxosId().toString()).orElse("undefined"));
//            });
//        });
//
//        replicasOperationsCallback.await(); // await for phase 1
//        logger.info("Commit transaction has successfully pre prepared all replicas groups");
        // we continue when we have all quorums
//        callbacksForPhase1.stre

        // For each replica group
            // 1. MppPrePrepareRequest - successfully register in MpPaxosIndex.
                // 1.1 each quorum of replicas should respond with paxos id
                // 1.2 each replica should make their transaction state consistent using MppService#makeTransactionDataConsistent ( this could happen later)
            // 2. MppPrepareRequest
                // send: (generated ballot + transaction state)
                // given transaction state, paxos id should be found in index. Else transaction was rolled back
                // access mp index -> find paxos id -> access MP Paxos State in system keyspace and find mp paxos state

    }

    public MpPrePrepareMpPaxosCallback prePrepareReplicaGroup(TransactionState transactionState, ReplicasGroupAndOwnedItems replicaGroupWithItems, ReplicasGroupsOperationCallback replicasOperationsCallback)
    {
        int targetReplicas = replicaGroupWithItems.getReplicasGroup().getReplicas().size();
        MpPrePrepareMpPaxosCallback callbackForPrePrepare = new MpPrePrepareMpPaxosCallback(targetReplicas, MPP_HARDCODED_CONSISTENCY_LEVEL, replicasOperationsCallback);
        MpPrePrepare mpPrePrepare = new MpPrePrepare(transactionState);
        MessageOut<MpPrePrepare> message = new MessageOut<>(MessagingService.Verb.MP_PAXOS_PRE_PREARE, mpPrePrepare, MpPrePrepare.serializer);

        replicaGroupWithItems.getReplicasGroup().getReplicas().stream().parallel().forEach(replica -> {
            messagingService.sendRR(message, replica, callbackForPrePrepare);
        });

        return callbackForPrePrepare;
    }

    public MpPrePrepareMpPaxosCallback prePrepareReplica(TransactionState transactionState, Replica replica, ReplicasGroupsOperationCallback replicasOperationsCallback)
    {
        int targetReplicas = 1;
        MpPrePrepareMpPaxosCallback callbackForPrePrepare = new MpPrePrepareMpPaxosCallback(targetReplicas, MPP_HARDCODED_CONSISTENCY_LEVEL, replicasOperationsCallback);
        MpPrePrepare mpPrePrepare = new MpPrePrepare(transactionState);
        MessageOut<MpPrePrepare> message = new MessageOut<>(MessagingService.Verb.MP_PAXOS_PRE_PREARE, mpPrePrepare, MpPrePrepare.serializer);
        messagingService.sendRR(message, replica.getHost(), callbackForPrePrepare);
        return callbackForPrePrepare;
    }

    public void rollbackTransaction(TransactionState transactionState)
    {
        // TODO [MPP][MPP-23] Implement it
        throw new NotImplementedException();
    }

    public void rollbackTransactionInternal(TransactionState transactionState, List<ReplicasGroupAndOwnedItems> replicasAndOwnedItems) {
        MessageOut<TransactionState> message = new MessageOut<>(MessagingService.Verb.MP_ROLLBACK, transactionState, TransactionStateSerializer.instance);
        for (ReplicasGroupAndOwnedItems replicasAndOwnedItem : replicasAndOwnedItems)
        {
            List<InetAddress> replicas = replicasAndOwnedItem.getReplicasGroup().getReplicas();
            for (InetAddress replica : replicas)
            {
                messagingService.sendOneWay(message, replica);
            }
        }
    }

    public void rollbackTransactionLocal(TransactionState transactionState)
    {
        TransactionId id = transactionState.id();
        if(privateMemtableStorage.transactionExistsInStorage(id)) {
            logger.info("Rollback transaction local tx id: {}" + id);
            privateMemtableStorage.removePrivateData(id);
        }
        mpPaxosIndex.acquireAndRollback(transactionState);
    }

    public Collection<TransactionId> getInProgressTransactions()
    {
        return privateMemtableStorage.getInProgressTransactions();
    }

    @Override
    public TransactionItem executeTransactionalMutationLocally(TransactionalMutation transactionalMutation)
    {
        final DecoratedKey key = getTransactionalMutationsKey(transactionalMutation);
        final Token token = key.getToken();
        final UUID transactionId = transactionalMutation.getTransactionId();

        logger.info("Execute transaction {} mutation's key is {} token is {} ", transactionId, key, token);
        privateMemtableStorage.storeMutation(new TransactionTimeUUID(transactionId), transactionalMutation.getMutation());

        final String cfName = getColumnFamilyName(transactionalMutation);

        return getTransactionItem(transactionalMutation, token, cfName);
    }

    private static DecoratedKey getTransactionalMutationsKey(TransactionalMutation transactionalMutation)
    {
        return transactionalMutation.getMutation().key();
    }

    private static TransactionItem getTransactionItem(TransactionalMutation transactionalMutation, Token token, String cfName)
    {
        return new TransactionItem(token, transactionalMutation.getKeyspaceName(), cfName);
    }

    public TransactionItem getTransactionItemForMutationNoExecution(TransactionalMutation transactionalMutation)
    {
        return getTransactionItem(transactionalMutation, getTransactionalMutationsKey(transactionalMutation).getToken(),
                                  getColumnFamilyName(transactionalMutation));
    }

    public boolean transactionExistsOnThisNode(TransactionId transactionId)
    {
        return privateMemtableStorage.transactionExistsInStorage(transactionId);
    }

    public TransactionState readLocalTransactionState(TransactionId transactionId)
    {
        logger.info("Execute readLocalTransactionState transactionId {} ", transactionId);
        final TransactionData transactionData = getTransactionData(transactionId);
        final Collection<TransactionItem> transactionItems = transactionData.asTransactionItems();

        final TransactionState transactionState = TransactionStateUtils.recreateTransactionState(transactionId, transactionItems);

        logger.info("Execute readLocalTransactionState transactionId {} returns transaction state ", transactionId, transactionState);

        return transactionState;
    }

    public void readAllByColumnFamily(TransactionId transactionId, String ksName, String cfName, Consumer<PartitionIterator> consumer)
    {
        logger.info("Execute readAllByColumnFamily transactionId: {} keyspaceName: {} columnFamilyName: {}", transactionId, ksName, cfName);
        processPartitions(transactionId, ksName, cfName, consumer, (cfId, transactionData) -> transactionData.partitionUpdatesStream(ksName, cfId));
    }

    public Optional<PartitionUpdate> readSingleTransactionItem(TransactionState transactionState)
    {
        logger.info("Execute read single partition item transactionState: {}", transactionState);
        Preconditions.checkArgument(transactionState.getTransactionItems().size() == 1);

        final TransactionItem transactionItem = transactionState.getTransactionItems().iterator().next();
        final TransactionId id = transactionState.id();

        final TransactionData transactionData = getTransactionData(id);
        final String ksName = transactionItem.getKsName();
        final UUID cfId = getCfId(transactionItem);

        final Optional<PartitionUpdate> partitionUpdateOpt = transactionData.readData(ksName, cfId, transactionItem.getToken());

        logger.info("Execute read single partition item transactionState: {} returns " +
                    (partitionUpdateOpt.isPresent() ? " partition update" : " not found partition update"), transactionState);

        return partitionUpdateOpt;
    }

    private static UUID getCfId(TransactionItem transactionItem)
    {
        return Schema.instance.getId(transactionItem.getKsName(), transactionItem.getCfName());
    }


    private void processPartitions(TransactionId transactionId, String ksName, String cfName, Consumer<PartitionIterator> consumer,
                                   BiFunction<UUID, TransactionData, Stream<PartitionUpdate>> getPartitionUpdates)
    {
        final TransactionData transactionData = getTransactionData(transactionId);
        final UUID cfId = Schema.instance.getId(ksName, cfName);

        final Stream<PartitionUpdate> partitionUpdateStream = getPartitionUpdates.apply(cfId, transactionData);
        processPartitionStream(consumer, partitionUpdateStream);
    }

    private void processPartitionStream(Consumer<PartitionIterator> consumer, Stream<PartitionUpdate> partitionUpdateStream)
    {
        int nowInSec = FBUtilities.nowInSeconds();
        final List<PartitionIterator> partitionIterators = partitionUpdateStream.map(pu -> pu.unfilteredIterator())
                                                                              .map(unfilteredI -> UnfilteredRowIterators.filter(unfilteredI, nowInSec))
                                                                              .map(PartitionIterators::singletonIterator)
                                                                              .collect(Collectors.toList());

        if (partitionIterators.isEmpty())
        {
            consumer.accept(EmptyIterators.partition());
        }
        else
        {
            try (final PartitionIterator partitionIterator = PartitionIterators.concat(partitionIterators))
            {
                consumer.accept(partitionIterator);
            }
        }
    }

    private TransactionData getTransactionData(TransactionId transactionId)
    {
        return privateMemtableStorage.readTransactionData(transactionId);
    }

    public void readAllByColumnFamilyAndToken(TransactionId transactionId, String ksName, String cfName, Token token, Consumer<PartitionIterator> consumer)
    {
        logger.info("Execute readAllByColumnFamilyAndToken transactionId: {} keyspaceName: {} columnFamilyName: {} token: {}", transactionId, ksName, cfName, token);
        processPartitions(transactionId, ksName, cfName, consumer, (cfId, transactionData) -> transactionData.readData(ksName, cfId, token).map(Stream::of).orElse(Stream.empty()));
    }

    public void readQuorumByColumnFamily(TransactionState transactionState, String ksName, String cfName, ConsistencyLevel consistencyLevel, Consumer<PartitionIterator> consumer)
    {
        logger.info("Execute readQuorumByColumnFamily transactionState: {} keyspaceName: {} columnFamilyName: {}", transactionState, ksName, cfName);

        final Stream<PartitionUpdate> partitionUpdateStream = transactionState.getTransactionItems().stream()
                                                                              .filter(ti -> ti.getKsName().equals(ksName) && ti.getCfName().equals(cfName))
                                                                              .map(ti -> {
                                                                                  final PartitionUpdate partitionUpdate = readSingleTransactionItemAndMergePartitionUpdates(transactionState, consistencyLevel, ti);
                                                                                  return partitionUpdate;
                                                                              });
        processPartitionStream(consumer, partitionUpdateStream);
    }

    public void readQuorumByColumnFamilyAndToken(TransactionState transactionState, String ksName, String cfName, Token token, ConsistencyLevel consistency, Consumer<PartitionIterator> consumer)
    {
        logger.info("Execute readQuorumByColumnFamilyAndToken transactionState: {} keyspaceName: {} columnFamilyName: {} token: {}", transactionState, ksName, cfName, token);

        logger.info("MppServiceImpl#readQuorumByColumnFamilyAndToken");

        Optional<TransactionItem> txItemOpt = transactionState.findTransactionItem(ksName, cfName, token);
        if(!txItemOpt.isPresent()) {
            throw new RuntimeException(String.format("TransactionState %s does not have such item ksName %s cfName %s token %s", transactionState, ksName, cfName, token));
        }

        final TransactionItem transactionItem = txItemOpt.get();

        final PartitionUpdate partitionUpdate = readSingleTransactionItemAndMergePartitionUpdates(transactionState, consistency, transactionItem);

        processPartitionStream(consumer, Stream.of(partitionUpdate));
    }

    /**
     * Flushes only those transaction items which are after last truncated date
     * @param transactionState
     * @param timestamp
     */
    @Override
    public void multiPartitionPaxosCommitPhase(TransactionState transactionState, long timestamp) {
        logger.info("flushTransaction executes for transaction id {}", transactionState.getTransactionId());
        Preconditions.checkState(privateMemtableStorage.transactionExistsInStorage(transactionState.id()), "TransactionData not found for transaction id %s", transactionState.getTransactionId());
        final TransactionData transactionData = getTransactionData(transactionState.id());

        logger.debug("transactionData.applyAllMutationsIfNotTruncated. TxId {}", transactionState.getTransactionId());
        transactionData.applyAllMutationsIfNotTruncated(timestamp);
        mpPaxosIndex.acquireAndMarkAsCommitted(transactionState, timestamp);
        mpPaxosIndex.acquireAndRemoveSelf(transactionState);
        // TODO [MPP] Private data should also have a retension, probably of same length as paxos state.
        // It can be deleted if none of replicas miss Most Recent Commit
        // It can be deleted on next paxos round
        privateMemtableStorage.removePrivateData(transactionState.id());
        logger.info("multiPartitionPaxosCommitPhase is done. TxId {}", transactionState.getTransactionId());
        jmxAddToCommitted(transactionState);
    }

    public void submitHints(List<MppHint> hints)
    {
        for( MppHint hint: hints) {
            TransactionData transactionData = privateMemtableStorage.readTransactionData(hint.getId());
            Preconditions.checkState(transactionData.isFrozen(), "Transaction data of tx id: " + hint.getId().unwrap() + " should be frozen in order to make hint");
            List<Mutation> mutations = transactionData.createMutationsForItems(hint.getItemsToHint(), hint.getTimestamp());
            for (Mutation mutation : mutations)
            {
                hintsService.submitHint(mutation, hint.getDestination());
            }
            logger.info("Hints [{}] about transaction {} submitted to destination {}", mutations.size(), hint.getId().unwrap(), hint.getDestination());
        }
    }

    public void flushTransactionLocally(TransactionId transactionId)
    {
        logger.info("flushTransactionLocally executes for transaction id {}", transactionId);

        // Normally it should be timestamp that comes from Paxos Ballot during Paxos Commit phase.
        final long timestampOfWrite = FBUtilities.timestampMicros();

        Preconditions.checkState(privateMemtableStorage.transactionExistsInStorage(transactionId), "TransactionData not found for transaction id %s", transactionId);
        final TransactionData transactionData = getTransactionData(transactionId);

        transactionData.applyAllMutations(timestampOfWrite);

        privateMemtableStorage.removePrivateData(transactionId);
    }

    private PartitionUpdate readSingleTransactionItemAndMergePartitionUpdates(TransactionState transactionState, ConsistencyLevel consistency, TransactionItem transactionItem)
    {
        final List<PartitionUpdate> partitionUpdates = readTransactionDataService.readSingleTransactionItem(transactionState.id(), transactionItem, consistency).get(transactionItem);

        return PartitionUpdate.merge(partitionUpdates);
    }

    private static String getColumnFamilyName(TransactionalMutation transactionalMutation)
    {
        final Collection<UUID> columnFamilyIds = transactionalMutation.getColumnFamilyIds();
        Preconditions.checkArgument(columnFamilyIds.size() == 1, "TransactionalMutation should modify only single table");
        final UUID cfId = columnFamilyIds.iterator().next();
        return Keyspace.open(transactionalMutation.getKeyspaceName()).getColumnFamilyStore(cfId).getTableName();
    }


    @Override
    public void makeTransactionDataConsistent(TransactionState transactionState)
    {
        TransactionData transactionData = privateMemtableStorage.readTransactionData(transactionState.id());
        transactionData.assertThatIsNotBeingMadeConsistent();
        transactionData.setThatItIsBeingMadeConsistent();
        if(transactionNotFrozenAlready(transactionState)) {
            Map<TransactionItem, List<PartitionUpdate>> itemWithPartitionUpdates = readTransactionDataService.readRelevantForThisNode(transactionState,
                                                                                                                                      ConsistencyLevel.LOCAL_TRANSACTIONAL);
            Preconditions.checkState(!itemWithPartitionUpdates.isEmpty(), "should have at least 1 item, otherwise this node wound't be contacted");

            Stream<Mutation> mutationsStream = itemWithPartitionUpdates.entrySet().stream().map(itemAndPartitionUpdates -> {
                List<PartitionUpdate> partitionUpdates = itemAndPartitionUpdates.getValue();
                PartitionUpdate consistentUpdate = PartitionUpdate.merge(partitionUpdates);
                return new Mutation(consistentUpdate);
            });

            mutationsStream.forEach(mutation -> {
                privateMemtableStorage.storeMutation(transactionState.id(), mutation);
            });

            logger.info("TransactionData is consistent TxID {}", transactionState.getTransactionId());
            privateMemtableStorage.freezeTransaction(transactionState.id());
        }
    }

    public Optional<MpPaxosId> prePrepareMultiPartitionPaxos(MpPrePrepare prePrepare)
    {
        logger.info("prePrepareMultiPartitionPaxos begins: transaction id: {}", prePrepare.getTransactionState().getTransactionId());
        Optional<MpPaxosId> mpPaxosId = mpPaxosIndex.acquireForMppPaxos(prePrepare.getTransactionState());
        logger.info("prePrepareMultiPartitionPaxos making transaction data consistent. TxID {}", prePrepare.getTransactionState().getTransactionId());
        makeTransactionDataConsistent(prePrepare.getTransactionState());

        logger.info("prePrepareMultiPartitionPaxos begins: transaction id: {}, is paxos round present: {}, paxos id is: {}", prePrepare.getTransactionState().getTransactionId(), mpPaxosId.isPresent(), mpPaxosId.map(id -> id.getPaxosId().toString()).orElse("not defined"));
        return mpPaxosId;
    }

    private boolean transactionNotFrozenAlready(TransactionState transactionState)
    {
        return !privateMemtableStorage.readTransactionData(transactionState.id()).isFrozen();
    }

    @Override
    public void makeTransactionDataConsistent(String transactionStateAsJson)
    {
        TransactionState transactionState = readTransactionStateFromJson(transactionStateAsJson);
        makeTransactionDataConsistent(transactionState);
    }

    private static TransactionState readTransactionStateFromJson(String transactionStateAsJson)
    {
        TransactionStateDto transactionStateDto = null;
        try
        {
            transactionStateDto = Json.JSON_OBJECT_MAPPER.readValue(transactionStateAsJson, TransactionStateDto.class);
        }
        catch (IOException e)
        {
            logger.error("Cannot create TransactionStateDto from json", e);
            throw new RuntimeException(e);
        }
        return TransactionStateUtils.compose(transactionStateDto);
    }

    private static TransactionItem readTransactionItemFromJson(String transactionItemAsJson)
    {
        TransactionItemDto transactionItemDto = null;
        try
        {
            transactionItemDto = Json.JSON_OBJECT_MAPPER.readValue(transactionItemAsJson, TransactionItemDto.class);
        }
        catch (IOException e)
        {
            logger.error("Cannot create TransactionItemDto from json", e);
            throw new RuntimeException(e);
        }
        return TransactionStateUtils.composeItem(transactionItemDto);
    }

    public void deleteSingleItem(String transactionStateAsJson, String transactionItemAsJson)
    {
        logger.info("deleteSingleItem tx {} ti {}", transactionStateAsJson, transactionItemAsJson);
        TransactionState transactionState = readTransactionStateFromJson(transactionStateAsJson);
        TransactionItem transactionItem = readTransactionItemFromJson(transactionItemAsJson);

        Preconditions.checkArgument(transactionState.hasItem(transactionItem),
                                    "Cannot delete transaction item that does not belong to transaction. TransactionState is: " +
                                    transactionState + " and item: " + transactionItem);

        purgeTransactionItem(transactionState, transactionItem);

        logger.info("DONE: deleted single transaction item tx {} ti {}", transactionStateAsJson, transactionItemAsJson);
    }

    public void storageProxyExtAddToWaitUntilAfterPrePrepared(String ... transactionIds)
    {
        // TODO [MPP] Maybe delete it. Not used any more.
    }

    Queue<String> listOfCommittedTransactions = new ConcurrentLinkedQueue<>();

    Queue<String> listOfRolledBackTransactions = new ConcurrentLinkedQueue<>();

    Queue<String> listOfMixedTransactions = new ConcurrentLinkedQueue<>();

    synchronized private void jmxAddToCommitted(TransactionState tx) {
        String txId = tx.getTransactionId().toString();
        listOfCommittedTransactions.add(txId);
        listOfMixedTransactions.add("C|"+txId);
    }

    synchronized public void jmxAddToRolledBack(TransactionState tx) {
        String txId = tx.getTransactionId().toString();
        listOfRolledBackTransactions.add(txId);
        listOfMixedTransactions.add("R|"+txId);
    }

    public void clearLists() {
        listOfCommittedTransactions = new ConcurrentLinkedQueue<>();
        listOfRolledBackTransactions = new ConcurrentLinkedQueue<>();
        listOfMixedTransactions = new ConcurrentLinkedQueue<>();
    }

    public Integer countReplicaGroupsForTransaction(String transactionStateAsJson)
    {
        TransactionState transactionState = readTransactionStateFromJson(transactionStateAsJson);
        List<ReplicasGroupAndOwnedItems> replicasGroupAndOwnedItemses = ForEachReplicaGroupOperations.groupItemsByReplicas(transactionState);
        List<ReplicasGroup> replicaGroups = replicasGroupAndOwnedItemses.stream().map(ReplicasGroupAndOwnedItems::getReplicasGroup).collect(Collectors.toList());
        logger.debug("countReplicaGroupsForTransaction for tx {} has replica groups {}", transactionState.getTransactionId(), replicaGroups);
        return replicaGroups.size();
    }

    public String [] listOfCommittedTransactions()
    {
        return asArr(listOfCommittedTransactions);
    }

    private String[] asArr(Queue<String> list)
    {
        ArrayList<String> copy = new ArrayList<>(list);
        return copy.toArray(new String [copy.size()]);
    }

    public String [] listOfRolledBackTransactions()
    {
        return asArr(listOfRolledBackTransactions);
    }

    public String [] listOfCommittedAndRolledBack()
    {
        return asArr(listOfMixedTransactions);
    }

    private void purgeTransactionItem(TransactionState transactionState, TransactionItem transactionItem)
    {
        privateMemtableStorage.readTransactionData(transactionState.id())
                              .purge(transactionItem.getKsName(), getCfId(transactionItem), transactionItem.getToken());
    }

    public Optional<MpPaxosId> acquireForMppPaxos(TransactionState transactionState)
    {
        return mpPaxosIndex.acquireForMppPaxos(transactionState);
    }

    public Optional<MpPaxosId> acquireAndFindPaxosId(TransactionState transactionState)
    {
        return mpPaxosIndex.acquireAndFindPaxosId(transactionState);
    }
}
