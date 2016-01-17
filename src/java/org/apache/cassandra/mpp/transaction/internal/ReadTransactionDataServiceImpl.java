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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.mpp.transaction.ReadTransactionDataService;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.mpp.transaction.network.MppMessageReceipient;
import org.apache.cassandra.mpp.transaction.network.MppNetworkService;
import org.apache.cassandra.mpp.transaction.network.MppResponseMessage;
import org.apache.cassandra.mpp.transaction.network.QuorumMppMessageResponseExpectations;
import org.apache.cassandra.mpp.transaction.network.messages.QuorumReadRequest;
import org.apache.cassandra.mpp.transaction.network.messages.QuorumReadResponse;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

/**
 * Service that reads private memtables of given transaction for this node.
 * It performs READ of PrivateMemtables with CL=QUORUM
 *
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 29/11/15
 */
public class ReadTransactionDataServiceImpl implements ReadTransactionDataService
{
    private MppNetworkService mppNetworkService;

    private static final long TIMEOUT_FOR_READING = 400;

    private static final Logger logger = LoggerFactory.getLogger(ReadTransactionDataServiceImpl.class);

    private static class ItemWithAddresses {
        private final TransactionItem txItem;

        private final Collection<InetAddress> endPoints;

        private final Integer replicationFactor;

        private ItemWithAddresses(TransactionItem txItem, Collection<InetAddress> endPoints, int replicationFactor)
        {
            this.txItem = txItem;
            this.endPoints = endPoints;
            this.replicationFactor = replicationFactor;
        }
    }

    public static class TransactionItemToUpdates
    {
        private final Map<TransactionItem, List<PartitionUpdate>> txItemToUpdates;

        private TransactionItemToUpdates(Map<TransactionItem, List<PartitionUpdate>> txItemToUpdates)
        {
            this.txItemToUpdates = txItemToUpdates;
        }

        public TransactionItemToUpdates merge(TransactionItemToUpdates m) {
            Map<TransactionItem, List<PartitionUpdate>> mergedMap = new HashMap<>(txItemToUpdates);
            m.txItemToUpdates.entrySet().forEach(entry -> mergedMap.merge(entry.getKey(), entry.getValue(), (v1, v2) -> {
                final List<PartitionUpdate> collect = Stream.concat(v1.stream(), v2.stream()).collect(Collectors.toList());
                return collect;
            }));
            return new TransactionItemToUpdates(mergedMap);
        }

    }


    /**
     * Invoked when data has to be read from this node and it's replicas for sake of consistency.
     *
     * Then received {@link TransactionDataPart} is guaranteed to be consistent.
     * @param transactionState
     * @return
     */
    public CompletableFuture<TransactionDataPart> readTransactionDataUsingQuorum(TransactionState transactionState) {

        final Stream<ItemWithAddresses> itemsWithAddresses = mapTransactionItemsToTheirEndpoints(transactionState);
        // Each item belongs to this node plus some other nodes.
        final Stream<ItemWithAddresses> ownedByThisNode = filterItemsWhichBelongToThisNode(itemsWithAddresses);

        final Map<Collection<InetAddress>, Map<Integer, List<ItemWithAddresses>>> groupedByReplicasAndByReplicatonFactor = ownedByThisNode.collect(Collectors.groupingBy(x -> x.endPoints, Collectors.groupingBy(x -> x.replicationFactor)));

        final Integer numberOfDifferentReplicationFactors = groupedByReplicasAndByReplicatonFactor.entrySet().stream().map(e -> e.getValue().keySet().size()).reduce(0, (a, b) -> a + b);
        int totalNumberOfRequests = numberOfDifferentReplicationFactors * groupedByReplicasAndByReplicatonFactor.keySet().size();
        assert totalNumberOfRequests > 0;
        logger.debug("Will do {} requests to read tx {}", totalNumberOfRequests, transactionState.getTransactionId());

        final List<CompletableFuture<Collection<MppResponseMessage>>> futures = groupedByReplicasAndByReplicatonFactor.entrySet().stream().flatMap(e -> {
            return e.getValue().entrySet().stream().map(v -> {
                final QuorumMppMessageResponseExpectations q = new QuorumMppMessageResponseExpectations(v.getKey());
                final List<TransactionItem> transactionItems = v.getValue().stream().map(x -> x.txItem).collect(Collectors.toList());
                final QuorumReadRequest quorumReadRequest = new QuorumReadRequest(transactionState.getTransactionId(), transactionItems);

                final Collection<InetAddress> receipientsAddresses = e.getKey();

                final CompletableFuture<Collection<MppResponseMessage>> collectionCompletableFuture = mppNetworkService.sendMessage(quorumReadRequest, q, getReceipients(receipientsAddresses)).getResponseFuture();
                return collectionCompletableFuture;
            });
        }).collect(Collectors.toList());

        final CompletableFuture<Collection<MppResponseMessage>> futureOfAllResults = collectionOfFuturesToFutureOfCollection(futures);

        return futureOfAllResults.thenApplyAsync(allResults -> {
            TransactionItemToUpdates m = new TransactionItemToUpdates(new HashMap<>());
            final Map<TransactionItem, List<PartitionUpdate>> mergedTxItemToUpdates = allResults.stream().map(x -> (QuorumReadResponse) x)
                                                                                          .map(x -> new TransactionItemToUpdates(x.getItems()))
                                                                                          .reduce(m, TransactionItemToUpdates::merge).txItemToUpdates;

            final Stream<Pair<TransactionItem, PartitionUpdate>> pairStream = ownedByThisNode.map(x -> x.txItem).map(txItem -> {
                final List<PartitionUpdate> partitionUpdates = mergedTxItemToUpdates.get(txItem);
                // TODO not sure if this is correct way to go because I am not sure whether timestamps are correct
                final PartitionUpdate mergedPartitionUpdate = PartitionUpdate.merge(partitionUpdates);
                return Pair.create(txItem, mergedPartitionUpdate);
            });

            final Map<TransactionItem, PartitionUpdate> ownedData = pairStream.collect(Collectors.toMap(p -> p.left, p -> p.right));

            return (TransactionDataPart) () -> ownedData;
        });

        /**
         * 1. Filter transaction items to only those owned by this replica
         * 2. Read this and other replicas.
         * 3. Wait for majority (so probably this one plus another one if CL = 3)
         * 4. Given majority responses we have to do merge of results.
         *      Group by keyspace and by key, then add mutations from all responses (can just create new TransactionData and then add all)
         * 5. Return that merged TransactionData which should be valid to be applied locally.
         */
    }

    private static CompletableFuture<Collection<MppResponseMessage>> collectionOfFuturesToFutureOfCollection(List<CompletableFuture<Collection<MppResponseMessage>>> futures)
    {
        CompletableFuture<Collection<MppResponseMessage>> allZeroArg = new CompletableFuture<>();
        allZeroArg.complete(Collections.emptyList());

        return futures.stream().reduce(allZeroArg, (f1, f2) -> f1.thenCombineAsync(f2, (r1, r2) -> {
            final Collection<MppResponseMessage> responses = new ArrayList<>();
            responses.addAll(r1);
            responses.addAll(r2);
            return responses;
        }));
    }

    private List<MppMessageReceipient> getReceipients(Collection<InetAddress> receipientsAddresses)
    {
        return receipientsAddresses.stream().map(mppNetworkService::createReceipient).collect(Collectors.toList());
    }

    private static Map<Collection<InetAddress>, List<TransactionItem>> groupBySameSetOfReplicas(Stream<ItemWithAddresses> ownedByThisNode)
    {
        return ownedByThisNode.collect(Collectors.groupingBy(x -> x.endPoints, Collectors.mapping(i -> i.txItem, Collectors.toList())));
    }

    /**
     * @return stream of items with corresponding endpoints
     */
    private static Stream<ItemWithAddresses> mapTransactionItemsToTheirEndpoints(TransactionState transactionState)
    {
        return transactionState.getTransactionItems().stream().map(mapTransactionItemToEndpoints());
    }

    private static Function<TransactionItem, ItemWithAddresses> mapTransactionItemToEndpoints()
    {
        return ti -> {
            final AbstractReplicationStrategy replicationStrategy = Keyspace.open(ti.getKsName()).getReplicationStrategy();
            final ArrayList<InetAddress> naturalEndpoints = replicationStrategy.getNaturalEndpoints(ti.getToken());
            final Collection<InetAddress> pending = StorageService.instance.getTokenMetadata().pendingEndpointsFor(ti.getToken(), ti.getKsName());
            final Iterable<InetAddress> addresses = Iterables.concat(naturalEndpoints, pending);
            return new ItemWithAddresses(ti, Lists.newArrayList(addresses), replicationStrategy.getReplicationFactor());
        };
    }

    /**
     * @return transaction items for which this node is responsible.
     */
    private static Stream<ItemWithAddresses> filterItemsWhichBelongToThisNode(Stream<ItemWithAddresses> itemsWithAddresses)
    {
        final InetAddress broadcastAddress = FBUtilities.getBroadcastAddress();
        return itemsWithAddresses.filter(item -> item.endPoints.contains(broadcastAddress));
    }
}
