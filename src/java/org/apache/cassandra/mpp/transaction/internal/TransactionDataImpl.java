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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.mpp.transaction.TransactionData;
import org.apache.cassandra.mpp.transaction.TransactionId;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 01/11/15
 */
public class TransactionDataImpl implements TransactionData
{
    private final TransactionId txId;
    private final long creationNano = System.nanoTime();

    private volatile boolean hasBeenApplied = false;
    private volatile boolean frozen = false;

    // TODO [MPP] this will be changed to actual private memtables, or maybe not.
    private final Map<String, Map<DecoratedKey, Mutation>> ksToKeyToMutation = new HashMap<>();

    public TransactionDataImpl(TransactionId txId)
    {
        this.txId = txId;
    }

    @Override
    public TransactionId getTxId()
    {
        return txId;
    }

    public boolean isExpired(int ttl)
    {
        return ttl > 0 && (System.nanoTime() - creationNano >= TimeUnit.MILLISECONDS.toNanos(ttl));
    }

    public void purge(String ksName, UUID cfId, Token token) {
        Map<DecoratedKey, Mutation> forKeyspace = ksToKeyToMutation.get(ksName);
        Map<DecoratedKey, Mutation> withoutModificationsOfCfForToken = forKeyspace.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, v -> {
            if (v.getValue().key().getToken().equals(token))
            {
                return v.getValue().without(cfId);
            }
            else
            {
                return v.getValue();
            }
        }));
        ksToKeyToMutation.put(ksName, withoutModificationsOfCfForToken);
    }

    public void addMutation(Mutation mutation)
    {
        Preconditions.checkState(!hasBeenApplied, "Cannot add mutation to already applied transaction's data. There could be some race condition");
        Preconditions.checkState(!frozen, "Cannot add mutation to frozen transaction's data. ");
        final String ksName = mutation.getKeyspaceName();

        final Map<DecoratedKey, Mutation> keyToMutation = ksToKeyToMutation.get(ksName);
        if (keyToMutation != null)
        {
            final Mutation mutationToMergeWith = keyToMutation.get(mutation.key());

            Mutation toPut;
            if (mutationToMergeWith != null)
            {
                // merge and put
                toPut = Mutation.merge(Arrays.asList(mutationToMergeWith, mutation));
            }
            else
            {
                toPut = mutation;
            }

            keyToMutation.put(toPut.key(), toPut);
        }
        else
        {
            Map<DecoratedKey, Mutation> newKeyToMutationsMap = new HashMap<>();
            newKeyToMutationsMap.put(mutation.key(), mutation);
            ksToKeyToMutation.put(ksName, newKeyToMutationsMap);
        }
    }

    public Stream<TransactionItem> asTransactionItemsStream() {
        final Stream<TransactionItem> transactionItemStream1 = getMutationStream().flatMap(m -> {
            final Stream<TransactionItem> transactionItemStream = m.getColumnFamilyIds().stream().map(cfIf -> Keyspace.open(m.getKeyspaceName()).getColumnFamilyStore(cfIf).getTableName()).map(cfName ->
                                                                                                                                                                                                new TransactionItem(m.key().getToken(), m.getKeyspaceName(), cfName)
            );
            return transactionItemStream;
        });

       return transactionItemStream1;
    }

    @Override
    public Collection<TransactionItem> asTransactionItems() {
        return asTransactionItemsStream().collect(Collectors.toSet());
    }

    public Collection<String> modifiedCfs()
    {
        return getMutations().stream().flatMap(m -> m.getPartitionUpdates().stream().map(x -> x.metadata().cfName)).collect(Collectors.toSet());
    }

    @Override
    public Stream<PartitionUpdate> partitionUpdatesStream(String ksName, UUID cfId) {
        Preconditions.checkNotNull(ksName, "Keyspace name is required");
        Preconditions.checkNotNull(cfId, "ColumnFamily name is required");
        final Map<DecoratedKey, Mutation> keysToMutation = ksToKeyToMutation.get(ksName);

        return keysToMutation.entrySet().stream()
                                 .filter(e -> modifiesColumnFamily(cfId, e))
                                 .map(v->v.getValue().getPartitionUpdate(cfId));
    }

    public void applyAllMutations(long applyTimestamp)
    {
        Preconditions.checkArgument(!hasBeenApplied, "Cannot apply same transaction data twice");
        ksToKeyToMutation.entrySet().stream().map(Map.Entry::getValue).flatMap(m -> m.entrySet().stream().map(Map.Entry::getValue))
                         .map(Mutation::copy) // copy, to refresh createdAt which is used to determine rpc timeout
                         .map(m -> {
                             // Update timestamps of all partition updates
                             m.getPartitionUpdates().forEach(pu -> pu.updateAllTimestamp(applyTimestamp));
                             return m;
                         })
                         .forEach(mutation -> mutation.apply());

        // Mutations happen in different thread so this method returns before actually doing mutations.
        // It is done same way for paxos and others therefore it should be enough and should not fail.
        hasBeenApplied = true;
    }

    public void freeze()
    {
        frozen = true;
    }

    public boolean isFrozen()
    {
        return frozen;
    }

    @Override
    public Optional<PartitionUpdate> readData(String ksName, UUID cfId, Token token)
    {
        final Map<DecoratedKey, Mutation> keysToMutation = ksToKeyToMutation.get(ksName);

        final List<PartitionUpdate> partitionUpdates = keysToMutation.entrySet()
                                                              .stream()
                                                              .filter(e -> sameToken(token, e) && modifiesColumnFamily(cfId, e))
                                                              .map(v->v.getValue().getPartitionUpdate(cfId)).collect(Collectors.toList());


        Preconditions.checkState(partitionUpdates.size() <= 1, "It may contain at most 1 PartitionUpdate for concrete CF and token");

        return partitionUpdates.stream().findFirst();
    }

    private static boolean modifiesColumnFamily(UUID cfId, Map.Entry<DecoratedKey, Mutation> e)
    {
        return e.getValue().getPartitionUpdate(cfId) != null;
    }

    private static boolean sameToken(Token token, Map.Entry<DecoratedKey, Mutation> e)
    {
        return e.getKey().getToken().equals(token);
    }

    /**
     * TODO [MPP] What about timestamps in these mutations? Should I change something? Copy them?
     */
    public Collection<Mutation> getMutations()
    {
        final Stream<Mutation> mutationStream = getMutationStream();
        final List<Mutation> allMutations = mutationStream
                                            .collect(Collectors.toList());
        return Collections.unmodifiableCollection(allMutations);
    }

    private Stream<Mutation> getMutationStream()
    {
        return (Stream<Mutation>) ksToKeyToMutation.entrySet()
                                                   .stream()
                                                   .flatMap(e -> e.getValue().entrySet().stream().map(Map.Entry::getValue));
    }
}
