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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.mpp.transaction.ReadTransactionDataRequestExecutor;
import org.apache.cassandra.mpp.transaction.ReadTransactionDataService;
import org.apache.cassandra.mpp.transaction.TransactionId;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.mpp.transaction.client.TransactionStateUtils;
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
        final Stream<ReadRequestRecipe> onlyRelevantForThisNode = ForEachReplicaGroupOperations.filterStreamItemsWhichBelongToThisNode(recipes.stream());

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
         *   This is no longer valid, because inserts to private memtables must succeed or client of API did something wrong
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
            return transactionItemsToBeReadOnEachReceipient.stream().map(item -> new SingleReadRequestRecipe(transactionId, item, replicationFactor, receipients));
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
    public static Collection<ReadRequestRecipe> prepareRequests(TransactionState transactionState) {

        final Stream<TransactionItemWithAddresses> withAddresses = ForEachReplicaGroupOperations.mapTransactionItemsToTheirEndpoints(transactionState);


        final Map<List<InetAddress>, Map<Integer, List<TransactionItemWithAddresses>>> groupedByReplicasAndByReplicatonFactor = withAddresses.collect(Collectors.groupingBy(x -> x.getEndPoints(), Collectors.groupingBy(x -> x.getReplicationFactor())));

        int totalNumberOfRequests = groupedByReplicasAndByReplicatonFactor.entrySet().stream().map(e -> e.getValue().keySet().size()).reduce(0, (a, b) -> a + b);
        assert totalNumberOfRequests > 0;
        logger.debug("Will do {} requests to read tx {}", totalNumberOfRequests, transactionState.getTransactionId());

        final List<ReadRequestRecipe> requests = groupedByReplicasAndByReplicatonFactor.entrySet().stream().flatMap(e -> e.getValue().entrySet().stream().map(v -> {
            final int replicationFactor = v.getKey();
            final List<TransactionItem> transactionItems = v.getValue().stream().map(x -> x.getTxItem()).collect(Collectors.toList());
            final List<InetAddress> receipientsAddresses = e.getKey();
            return new ReadRequestRecipe(transactionState.id(), replicationFactor, transactionItems, receipientsAddresses);
        })).collect(Collectors.toList());
        return requests;
    }
}
