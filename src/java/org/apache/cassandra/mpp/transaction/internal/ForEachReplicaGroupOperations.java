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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.mpp.transaction.network.MppResponseMessage;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 04/04/16
 */
public class ForEachReplicaGroupOperations
{
    private static final Logger logger = LoggerFactory.getLogger(ForEachReplicaGroupOperations.class);

    // TODO [MPP] Assumption is that replication factor = 3
    public static List<ReplicasGroupAndOwnedItems> groupItemsByReplicas(TransactionState transactionState) {
        final Stream<TransactionItemWithAddresses> withAddresses = ForEachReplicaGroupOperations.mapTransactionItemsToTheirEndpoints(transactionState);

        List<TransactionItemWithAddresses> collected = withAddresses.collect(Collectors.toList());

        Map<String, Replica> hostAddressToReplica = collected.stream().flatMap(ti -> ti.getEndPoints().stream())
                                                             .map(Replica::new)
                                                             .distinct()
                                                             .collect(Collectors.toMap(Replica::getHostAddress, Function.identity()));

        Map<List<String>, List<TransactionItemWithAddresses>> groupedBySameReplicas = collected.stream().map(txItemWithAddress -> {
            List<String> grouping = txItemWithAddress.getEndPoints().stream().map(InetAddress::getHostAddress).sorted().collect(Collectors.toList());

            return Pair.create(txItemWithAddress, grouping);
        }).collect(Collectors.groupingBy(x -> x.right, Collectors.mapping(x -> x.left, Collectors.toList())));

        Map<List<InetAddress>, List<TransactionItemWithAddresses>> itemsGroupedByReplicasGroup = groupedBySameReplicas.entrySet().stream().collect(Collectors.toMap(entry -> {
            return entry.getValue().iterator().next().getEndPoints();
        }, entry -> entry.getValue()));


        List<ReplicasGroupAndOwnedItems> groupingByReplicas = itemsGroupedByReplicasGroup.entrySet().stream().map(v -> {
            List<InetAddress> replicasAddrs = v.getKey();
            List<TransactionItem> itemsOwnedByReplicasGroup = v.getValue().stream().map(TransactionItemWithAddresses::getTxItem).collect(Collectors.toList());

            List<Replica> replicas = replicasAddrs.stream().map(host -> hostAddressToReplica.get(host.getHostAddress())).collect(Collectors.toList());
            ReplicasGroup replicasGroup = new ReplicasGroup(replicas);
            ReplicasGroupAndOwnedItems replicasGroupAndOwnedItems = new ReplicasGroupAndOwnedItems(replicasGroup, itemsOwnedByReplicasGroup, hostAddressToReplica.values());
            return replicasGroupAndOwnedItems;
        }).collect(Collectors.toList());
        return groupingByReplicas;
    }

    public static int countNumberOfRequestsRequired(TransactionState transactionState) {
        final Stream<TransactionItemWithAddresses> ownedByThisNode = identifyTransactionItemsOwnedByThisNode(transactionState);

        final Map<Collection<InetAddress>, Map<Integer, List<TransactionItemWithAddresses>>> groupedByReplicasAndByReplicatonFactor = ownedByThisNode.collect(Collectors.groupingBy(x -> x.getEndPoints(), Collectors.groupingBy(x -> x.getReplicationFactor())));

        return groupedByReplicasAndByReplicatonFactor.entrySet().stream().map(e -> e.getValue().keySet().size()).reduce(0, (a, b) -> a + b);
    }

    /**
     * @return stream of items with corresponding endpoints
     */
    static Stream<TransactionItemWithAddresses> mapTransactionItemsToTheirEndpoints(TransactionState transactionState)
    {
        return transactionState.getTransactionItems().stream().map(mapTransactionItemToEndpoints());
    }

    static Stream<TransactionItemWithAddresses> mapTransactionItemsToAllEndpoints(TransactionState transactionState)
    {
        return transactionState.getTransactionItems().stream().map(mapTransactionItemToAliveOrDeadEndpoints());
    }

    static Function<TransactionItem, TransactionItemWithAddresses> mapTransactionItemToAliveOrDeadEndpoints()
    {
        return ti -> {
            final Keyspace keyspace = Keyspace.open(ti.getKsName());
            List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(keyspace.getName(), ti.getToken());
            Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(ti.getToken(), keyspace.getName());
            ImmutableList<InetAddress> allEndpoints = ImmutableList.copyOf(Iterables.concat(naturalEndpoints, pendingEndpoints));

//            List<InetAddress> allReplicas = StorageProxy.ge(keyspace, ti.getToken());
            final AbstractReplicationStrategy replicationStrategy = keyspace.getReplicationStrategy();
//            logger.info("ReadTransactionDataService transaction items to replicas. All replicas are {}", allReplicas);
            // TODO [MPP] I noticed that Write goes for natural + pending while reads are done just against natural using getLiveSortedEndpoints method in AbstractReadExecutor
//            final ArrayList<InetAddress> naturalEndpoints = replicationStrategy.getNaturalEndpoints(ti.getToken());
//            final Collection<InetAddress> pending = StorageService.instance.getTokenMetadata().pendingEndpointsFor(ti.getToken(), ti.getKsName());
//            final Iterable<InetAddress> addresses = Iterables.concat(naturalEndpoints, pending);
            return new TransactionItemWithAddresses(ti, allEndpoints, replicationStrategy.getReplicationFactor());
        };
    }

    static Function<TransactionItem, TransactionItemWithAddresses> mapTransactionItemToEndpoints()
    {
        return ti -> {
            final Keyspace keyspace = Keyspace.open(ti.getKsName());
            List<InetAddress> allReplicas = StorageProxy.getLiveSortedEndpoints(keyspace, ti.getToken());
            final AbstractReplicationStrategy replicationStrategy = keyspace.getReplicationStrategy();
//            logger.info("ReadTransactionDataService transaction items to replicas. All replicas are {}", allReplicas);
            // TODO [MPP] I noticed that Write goes for natural + pending while reads are done just against natural using getLiveSortedEndpoints method in AbstractReadExecutor
//            final ArrayList<InetAddress> naturalEndpoints = replicationStrategy.getNaturalEndpoints(ti.getToken());
//            final Collection<InetAddress> pending = StorageService.instance.getTokenMetadata().pendingEndpointsFor(ti.getToken(), ti.getKsName());
//            final Iterable<InetAddress> addresses = Iterables.concat(naturalEndpoints, pending);
            return new TransactionItemWithAddresses(ti, allReplicas, replicationStrategy.getReplicationFactor());
        };
    }

    public static Stream<TransactionItemWithAddresses> identifyTransactionItemsOwnedByThisNode(TransactionState transactionState)
    {
        final Stream<TransactionItemWithAddresses> itemsWithAddresses = mapTransactionItemsToTheirEndpoints(transactionState);
        // Each item belongs to this node plus some other nodes.
        return filterStreamItemsWhichBelongToThisNode(itemsWithAddresses);
    }

    /**
     * @return transaction items for which this node is responsible.
     */
    public static <T extends WithEndpoints> Stream<T> filterStreamItemsWhichBelongToThisNode(Stream<T> itemsWithAddresses)
    {
        final InetAddress broadcastAddress = FBUtilities.getBroadcastAddress();
        return itemsWithAddresses.filter(item -> item.getEndPoints().contains(broadcastAddress));
    }

    public static Function<TransactionState, Stream<TransactionItem>> TRANSACTION_ITEMS_OWNED_BY_THIS_NODE = transactionState -> identifyTransactionItemsOwnedByThisNode(transactionState).map(TransactionItemWithAddresses::getTxItem);

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

    private static Map<Collection<InetAddress>, List<TransactionItem>> groupBySameSetOfReplicas(Stream<TransactionItemWithAddresses> ownedByThisNode)
    {
        return ownedByThisNode.collect(Collectors.groupingBy(TransactionItemWithAddresses::getEndPoints, Collectors.mapping(TransactionItemWithAddresses::getTxItem, Collectors.toList())));
    }

    static <K, T> Map<K, List<T>> mergeByKey(Stream<Map<K, T>> maps) {
        return maps.flatMap(v -> v.entrySet().stream()).collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.mapping(Map.Entry::getValue, Collectors.toList())));
    }

    static boolean isQuorumSatisfied(int replicationFactor, Collection<TransactionItem> transactionItems,
                                     Collection<Map<TransactionItem, Optional<PartitionUpdate>>> transactionItemToPartitionUpdate) {
        final List<TransactionItem> transactionItemsForWhichQuorumFailed = getTransactionItemsForWhichQuorumIsNotSatisfied(replicationFactor, transactionItemToPartitionUpdate);
        return transactionItemsForWhichQuorumFailed.isEmpty();
    }

    private static List<TransactionItem> getTransactionItemsForWhichQuorumIsNotSatisfied(int replicationFactor, Collection<Map<TransactionItem, Optional<PartitionUpdate>>> transactionItemToPartitionUpdate)
    {
        final int quorum = quorumFor(replicationFactor);
        final Map<TransactionItem, List<Optional<PartitionUpdate>>> mergedPartitionUpdates = mergeByKey(transactionItemToPartitionUpdate.stream());

        return mergedPartitionUpdates.entrySet()
                                     .stream()
                                     .filter(kv -> kv.getValue()
                                                     .stream()
                                                     .filter(Optional::isPresent)
                                                     .count() < quorum)
                                     .map(Map.Entry::getKey)
                                     .collect(Collectors.toList());
    }


    public static int quorumFor(int replicationFactor) {
        return (replicationFactor / 2) + 1;
    }



}
