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
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    public void addMutation(Mutation mutation)
    {
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

    public Stream<PartitionUpdate> readData(String ksName, UUID cfId, Token token)
    {
        final Map<DecoratedKey, Mutation> keysToMutation = ksToKeyToMutation.get(ksName);

        final Stream<PartitionUpdate> partitionUpdates = keysToMutation.entrySet()
                                                              .stream()
                                                              .filter(e -> sameToken(token, e) && modifiesColumnFamily(cfId, e))
                                                              .map(v->v.getValue().getPartitionUpdate(cfId));
        return partitionUpdates;
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
