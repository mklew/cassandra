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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.mpp.transaction.ReadTransactionDataRequestExecutor;
import org.apache.cassandra.mpp.transaction.ReadTransactionDataService;
import org.apache.cassandra.mpp.transaction.TransactionId;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.mpp.transaction.client.TransactionStateUtils;
import org.apache.cassandra.mpp.transaction.network.MppResponseMessage;
import org.apache.cassandra.service.StorageProxy;
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
    private ReadTransactionDataRequestExecutor executor;

    public ReadTransactionDataRequestExecutor getExecutor()
    {
        return executor;
    }

    public void setExecutor(ReadTransactionDataRequestExecutor executor)
    {
        this.executor = executor;
    }

    private static final Logger logger = LoggerFactory.getLogger(ReadTransactionDataServiceImpl.class);

    @Override
    public Map<TransactionItem, List<PartitionUpdate>> readRelevantForThisNode(TransactionState transactionState, ConsistencyLevel consistencyLevel)
    {
        final Collection<ReadRequestRecipe> recipes = prepareRequests(transactionState);
        final Stream<ReadRequestRecipe> onlyRelevantForThisNode = filterStreamItemsWhichBelongToThisNode(recipes.stream());

        final Map<TransactionItem, List<PartitionUpdate>> updates = onlyRelevantForThisNode
                                                                          .parallel()
                                                                          .flatMap(ReadRequestRecipe::toSingleReadRequestsStream)
                                                                           // This is where requests happen
                                                                          .map(r -> executeSingleReadRequestRecipe(r, consistencyLevel))
                                                                          .collect(Collectors.toMap(p -> p.left, p -> p.right));

        return updates;
    }

    private Pair<TransactionItem, List<PartitionUpdate>> executeSingleReadRequestRecipe(SingleReadRequestRecipe singleReadRequestRecipe, ConsistencyLevel consistencyLevel) {
        final List<PartitionUpdate> partitionUpdates = executor.executeRecipe(singleReadRequestRecipe, consistencyLevel);

        Preconditions.checkState(singleReadRequestRecipe.isQuorum(partitionUpdates), "Quorum should be met");

        return Pair.create(singleReadRequestRecipe.getTransactionItem(), partitionUpdates);
    }

    @Override
    public Map<TransactionItem, List<PartitionUpdate>> readSingleTransactionItem(TransactionId transactionId,
                                                                                       TransactionItem transactionItem,
                                                                                       ConsistencyLevel consistencyLevel)
    {
        logger.info("ReadTransactionDataServiceImpl#readSingleTransactionItem transactionId {}", transactionId);
        final Collection<ReadRequestRecipe> readRequestRecipes = prepareRequests(TransactionStateUtils.recreateTransactionState(transactionId,
                                                                                                                                Collections.singletonList(transactionItem)));

        Preconditions.checkState(readRequestRecipes.size() == 1);

        final ReadRequestRecipe readRequestRecipe = readRequestRecipes.iterator().next();

        final Collection<SingleReadRequestRecipe> singleReadRequestRecipes = readRequestRecipe.toSingleReadRequests();
        Preconditions.checkState(singleReadRequestRecipes.size() == 1);

        final SingleReadRequestRecipe singleReadRequestRecipe = singleReadRequestRecipes.iterator().next();
        final Pair<TransactionItem, List<PartitionUpdate>> transactionItemCollectionPair = executeSingleReadRequestRecipe(singleReadRequestRecipe, consistencyLevel);
        logger.info("ReadTransactionDataServiceImpl#readSingleTransactionItem transactionId {} got response for transaction item {} with {} partition updates", transactionId,
                    transactionItemCollectionPair.left, transactionItemCollectionPair.right.size());

        return Collections.singletonMap(transactionItemCollectionPair.left, transactionItemCollectionPair.right);
    }

    interface WithEndpoints {
        List<InetAddress> getEndPoints();
    }

    public static class TransactionItemWithAddresses implements WithEndpoints
    {
        private final TransactionItem txItem;

        private final List<InetAddress> endPoints;

        private final Integer replicationFactor;

        private TransactionItemWithAddresses(TransactionItem txItem, List<InetAddress> endPoints, int replicationFactor)
        {
            this.txItem = txItem;
            this.endPoints = endPoints;
            this.replicationFactor = replicationFactor;
        }

        public TransactionItem getTxItem()
        {
            return txItem;
        }

        public List<InetAddress> getEndPoints()
        {
            return endPoints;
        }

        public Integer getReplicationFactor()
        {
            return replicationFactor;
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

    public int countNumberOfRequestsRequired(TransactionState transactionState) {
        final Stream<TransactionItemWithAddresses> ownedByThisNode = identifyTransactionItemsOwnedByThisNode(transactionState);

        final Map<Collection<InetAddress>, Map<Integer, List<TransactionItemWithAddresses>>> groupedByReplicasAndByReplicatonFactor = ownedByThisNode.collect(Collectors.groupingBy(x -> x.endPoints, Collectors.groupingBy(x -> x.replicationFactor)));

        return groupedByReplicasAndByReplicatonFactor.entrySet().stream().map(e -> e.getValue().keySet().size()).reduce(0, (a, b) -> a + b);
    }

    /**
     * This is response from receipient in ReadRequestRecipe, for
     *
     * ReadRequestRecipe
     *
     */
    public static class ReadRequestResponse {
        InetAddress from;

        Map<TransactionItem, Optional<PartitionUpdate>> txItemToPartitionUpdates;
    }

    public static class FullReadRequestResponse {

        Collection<ReadRequestResponse> responses;

        ReadRequestRecipe readRequestRecipe;
    }

    public static int quorumFor(int replicationFactor) {
        return (replicationFactor / 2) + 1;
    }

    // Partition
    public static Map<TransactionItem, Collection<PartitionUpdate>> filterPartitionUpdatesToOnlyThoseSeenByQuorumAndJoinThem(int replicationFactor,
                                                                                                                             Collection<Map<TransactionItem, Collection<PartitionUpdate>>> responses) {
        final int quorum = quorumFor(replicationFactor);

        // TODO PartitionUpdate key is partitioning key.
        // PartitionUpdate is also per specific CF -> Same for TransactionItem, therefore I can have One to One relationship instead of TxItem -> Collection<PartitionUpdate>


        // TODO Test TransactionData how it looks depending on what updates are inserted.

        return null;
    }



    public static class ReadRequestRecipe implements WithEndpoints {

        final TransactionId transactionId;

        /**
         * It has to wait for  (replicationFactor / 2) + 1 responses.
         * Messages are sent to all of receipients.
         *
         * Receipients might be > replicationFactor
         *
         * Number of live receipients must be >= (replicationFactor / 2) + 1
         */
        final int replicationFactor;

        /**
         * All of these transaction items are owned by receipients.
         * Receipients might or might not have correct data, but given quorum responses
         * it should be possible to reconstruct that data.
         *
         *
         * Case that has to be tested:
         *   there might be a write with same ks/cf/token, but it will be only at 1 node.
         *   TODO If there is no quorum responses for any given FULL KEY then this row has to be discarded.
         */
        final List<TransactionItem> transactionItemsToBeReadOnEachReceipient;

        final List<InetAddress> receipients;

        public ReadRequestRecipe(TransactionId transactionId, int replicationFactor, List<TransactionItem> transactionItemsToBeReadOnEachReceipient, List<InetAddress> receipients)
        {
            this.transactionId = transactionId;
            this.replicationFactor = replicationFactor;
            this.transactionItemsToBeReadOnEachReceipient = transactionItemsToBeReadOnEachReceipient;
            this.receipients = receipients;
        }

        public String toString()
        {
            return "ReadRequestRecipe{" +
                   "transactionId=" + transactionId +
                   ", replicationFactor=" + replicationFactor +
                   ", transactionItemsToBeReadOnEachReceipient=" + transactionItemsToBeReadOnEachReceipient +
                   ", receipients=" + receipients +
                   '}';
        }

        public List<InetAddress> getEndPoints()
        {
            return Collections.unmodifiableList(receipients);
        }

        Stream<SingleReadRequestRecipe> toSingleReadRequestsStream() {
            return transactionItemsToBeReadOnEachReceipient.stream().map(item -> {
                return new SingleReadRequestRecipe(transactionId, item, replicationFactor, receipients);
            });
        }

        Collection<SingleReadRequestRecipe> toSingleReadRequests()
        {
            return toSingleReadRequestsStream().collect(Collectors.toList());
        }
    }

    /**
     *
     * @param transactionState
     * @return recipes for all transaction items in transaction state.
     */
    public Collection<ReadRequestRecipe> prepareRequests(TransactionState transactionState) {

        final Stream<TransactionItemWithAddresses> withAddresses = mapTransactionItemsToTheirEndpoints(transactionState);

        final Map<List<InetAddress>, Map<Integer, List<TransactionItemWithAddresses>>> groupedByReplicasAndByReplicatonFactor = withAddresses.collect(Collectors.groupingBy(x -> x.endPoints, Collectors.groupingBy(x -> x.replicationFactor)));

        int totalNumberOfRequests = groupedByReplicasAndByReplicatonFactor.entrySet().stream().map(e -> e.getValue().keySet().size()).reduce(0, (a, b) -> a + b);
        assert totalNumberOfRequests > 0;
        logger.debug("Will do {} requests to read tx {}", totalNumberOfRequests, transactionState.getTransactionId());

        final List<ReadRequestRecipe> requests = groupedByReplicasAndByReplicatonFactor.entrySet().stream().flatMap(e -> e.getValue().entrySet().stream().map(v -> {
            final int replicationFactor = v.getKey();
            final List<TransactionItem> transactionItems = v.getValue().stream().map(x -> x.txItem).collect(Collectors.toList());
            final List<InetAddress> receipientsAddresses = e.getKey();
            return new ReadRequestRecipe(transactionState.id(), replicationFactor, transactionItems, receipientsAddresses);
        })).collect(Collectors.toList());
        return requests;
    }

    /**
     * Invoked when data has to be read from this node and it's replicas for sake of consistency.
     *
     * Then received TransactionDataPart is guaranteed to be consistent.
     * @param transactionState
     * @return
     */
//    public CompletableFuture<TransactionDataPart> readTransactionDataUsingQuorum(TransactionState transactionState) {
//
//        final Stream<TransactionItemWithAddresses> ownedByThisNode = identifyTransactionItemsOwnedByThisNode(transactionState);
//
//        final Map<Collection<InetAddress>, Map<Integer, List<TransactionItemWithAddresses>>> groupedByReplicasAndByReplicatonFactor = ownedByThisNode.collect(Collectors.groupingBy(x -> x.endPoints, Collectors.groupingBy(x -> x.replicationFactor)));
//
//        int totalNumberOfRequests = groupedByReplicasAndByReplicatonFactor.entrySet().stream().map(e -> e.getValue().keySet().size()).reduce(0, (a, b) -> a + b);
//        assert totalNumberOfRequests > 0;
//        logger.debug("Will do {} requests to read tx {}", totalNumberOfRequests, transactionState.getTransactionId());
//
//        final List<CompletableFuture<Collection<MppResponseMessage>>> futures = groupedByReplicasAndByReplicatonFactor.entrySet().stream().flatMap(e -> {
//            return e.getValue().entrySet().stream().map(v -> {
//                final QuorumMppMessageResponseExpectations q = new QuorumMppMessageResponseExpectations(v.getKey());
//                final List<TransactionItem> transactionItems = v.getValue().stream().map(x -> x.txItem).collect(Collectors.toList());
//                final QuorumReadRequest quorumReadRequest = new QuorumReadRequest(transactionState.getTransactionId(), transactionItems);
//
//                final Collection<InetAddress> receipientsAddresses = e.getKey();
//
//                final CompletableFuture<Collection<MppResponseMessage>> collectionCompletableFuture = mppNetworkService.sendMessage(quorumReadRequest, q, getReceipients(receipientsAddresses)).getResponseFuture();
//                return collectionCompletableFuture;
//            });
//        }).collect(Collectors.toList());
//
//        final CompletableFuture<Collection<MppResponseMessage>> futureOfAllResults = collectionOfFuturesToFutureOfCollection(futures);
//        // TODO [MPP] Reorganize code, it does not compile atm because signatures were changed
//        throw new RuntimeException("NOT IMPLEMENTED");
////        return futureOfAllResults.thenApplyAsync(allResults -> {
////            TransactionItemToUpdates m = new TransactionItemToUpdates(new HashMap<>());
////            final Map<TransactionItem, Optional<PartitionUpdate>> mergedTxItemToUpdates = allResults.stream().map(x -> (QuorumReadResponse) x)
////                                                                                          .map(x -> new TransactionItemToUpdates(x.getItems()))
////                                                                                          .reduce(m, TransactionItemToUpdates::merge).txItemToUpdates;
////
////            final Stream<Pair<TransactionItem, PartitionUpdate>> pairStream = ownedByThisNode.map(x -> x.txItem).map(txItem -> {
////                final List<PartitionUpdate> partitionUpdates = mergedTxItemToUpdates.get(txItem);
////                // TODO not sure if this is correct way to go because I am not sure whether timestamps are correct
////                final PartitionUpdate mergedPartitionUpdate = PartitionUpdate.merge(partitionUpdates);
////                return Pair.create(txItem, mergedPartitionUpdate);
////            });
////
////            final Map<TransactionItem, PartitionUpdate> ownedData = pairStream.collect(Collectors.toMap(p -> p.left, p -> p.right));
////
////            return (TransactionDataPart) () -> ownedData;
////        });
//
//        /**
//         * 1. Filter transaction items to only those owned by this replica
//         * 2. Read this and other replicas.
//         * 3. Wait for majority (so probably this one plus another one if CL = 3)
//         * 4. Given majority responses we have to do merge of results.
//         *      Group by keyspace and by key, then add mutations from all responses (can just create new TransactionData and then add all)
//         * 5. Return that merged TransactionData which should be valid to be applied locally.
//         */
//    }

    public static Stream<TransactionItemWithAddresses> identifyTransactionItemsOwnedByThisNode(TransactionState transactionState)
    {
        final Stream<TransactionItemWithAddresses> itemsWithAddresses = mapTransactionItemsToTheirEndpoints(transactionState);
        // Each item belongs to this node plus some other nodes.
        return filterStreamItemsWhichBelongToThisNode(itemsWithAddresses);
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

//    private List<MppMessageReceipient> getReceipients(Collection<InetAddress> receipientsAddresses)
//    {
//        return receipientsAddresses.stream().map(mppNetworkService::createReceipient).collect(Collectors.toList());
//    }

    private static Map<Collection<InetAddress>, List<TransactionItem>> groupBySameSetOfReplicas(Stream<TransactionItemWithAddresses> ownedByThisNode)
    {
        return ownedByThisNode.collect(Collectors.groupingBy(x -> x.endPoints, Collectors.mapping(i -> i.txItem, Collectors.toList())));
    }

    // TODO [MPP] VERY IMPORTANT One request for many transaction items cannot guarantee that responses from quorum of receipients satisfy quorum per transaction item -- data might be missing
    // Therefore it can either:
    //  1) send 1 request per TransactionItem and wait for quorum of responses
    //  2) send bundle, but WAIT TILL QUORUM IS SATISIFED FOR ALL TRANSACTION ITEMS, therefore if for each TransactionItem there is quorum of PartitionUpdates.
    // satisfy


    static <K, T> Map<K, List<T>> mergeByKey(Stream<Map<K, T>> maps) {
        return maps.flatMap(v -> v.entrySet().stream()).collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.mapping(Map.Entry::getValue, Collectors.toList())));
    }

    boolean isQuorumSatisfied(int replicationFactor, Collection<TransactionItem> transactionItems,
                              Collection<Map<TransactionItem, Optional<PartitionUpdate>>> transactionItemToPartitionUpdate) {
        final List<TransactionItem> transactionItemsForWhichQuorumFailed = getTransactionItemsForWhichQuorumIsNotSatisfied(replicationFactor, transactionItemToPartitionUpdate);
        return transactionItemsForWhichQuorumFailed.isEmpty();
    }

    private List<TransactionItem> getTransactionItemsForWhichQuorumIsNotSatisfied(int replicationFactor, Collection<Map<TransactionItem, Optional<PartitionUpdate>>> transactionItemToPartitionUpdate)
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


    /**
     * @return stream of items with corresponding endpoints
     */
    private static Stream<TransactionItemWithAddresses> mapTransactionItemsToTheirEndpoints(TransactionState transactionState)
    {
        return transactionState.getTransactionItems().stream().map(mapTransactionItemToEndpoints());
    }

    private static Function<TransactionItem, TransactionItemWithAddresses> mapTransactionItemToEndpoints()
    {
        return ti -> {
            final Keyspace keyspace = Keyspace.open(ti.getKsName());
            List<InetAddress> allReplicas = StorageProxy.getLiveSortedEndpoints(keyspace, ti.getToken());
            final AbstractReplicationStrategy replicationStrategy = keyspace.getReplicationStrategy();
            // TODO [MPP] I noticed that Write goes for natural + pending while reads are done just against natural using getLiveSortedEndpoints method in AbstractReadExecutor
//            final ArrayList<InetAddress> naturalEndpoints = replicationStrategy.getNaturalEndpoints(ti.getToken());
//            final Collection<InetAddress> pending = StorageService.instance.getTokenMetadata().pendingEndpointsFor(ti.getToken(), ti.getKsName());
//            final Iterable<InetAddress> addresses = Iterables.concat(naturalEndpoints, pending);
            return new TransactionItemWithAddresses(ti, allReplicas, replicationStrategy.getReplicationFactor());
        };
    }

    /**
     * @return transaction items for which this node is responsible.
     */
    private static <T extends WithEndpoints> Stream<T> filterStreamItemsWhichBelongToThisNode(Stream<T> itemsWithAddresses)
    {
        final InetAddress broadcastAddress = FBUtilities.getBroadcastAddress();
        return itemsWithAddresses.filter(item -> item.getEndPoints().contains(broadcastAddress));
    }



}
