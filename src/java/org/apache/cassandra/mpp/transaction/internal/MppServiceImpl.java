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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
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

    /**
     * It should block until end of transaction, and then return.
     *
     * @param transactionState
     * @param consistencyLevel
     */
    public void commitTransaction(TransactionState transactionState, ConsistencyLevel consistencyLevel)
    {
        logger.info("Commit transaction called with transaction state {} and consistency level {}", transactionState, consistencyLevel);

        // TODO [MPP] Implement it
        throw new NotImplementedException();
    }

    public void rollbackTransaction(TransactionState transactionState)
    {
        // TODO [MPP][MPP-23] Implement it
        throw new NotImplementedException();
    }

    public void rollbackTransactionLocal(TransactionState transactionState)
    {
        logger.info("Rollback transaction local txState: " + transactionState);
        privateMemtableStorage.removePrivateData(transactionState.id());
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

            privateMemtableStorage.freezeTransaction(transactionState.id());
        }
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

    private void purgeTransactionItem(TransactionState transactionState, TransactionItem transactionItem)
    {
        privateMemtableStorage.readTransactionData(transactionState.id())
                              .purge(transactionItem.getKsName(), getCfId(transactionItem), transactionItem.getToken());
    }
}
