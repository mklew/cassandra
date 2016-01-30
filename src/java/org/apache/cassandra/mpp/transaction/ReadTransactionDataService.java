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

import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;
import org.apache.cassandra.mpp.transaction.client.TransactionState;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 06/12/15
 */
public interface ReadTransactionDataService
{

    /**
     *
     * @param transactionState
     * @param consistencyLevel
     * @return items that are owned by this node with partition updates using quorum
     */
    Map<TransactionItem, List<PartitionUpdate>> readRelevantForThisNode(TransactionState transactionState, ConsistencyLevel consistencyLevel);

    /**
     * Method performs reads to responsible nodes, it waits for quorum of responses with PartitionUpdates for this node.
     *
     *
     * @param transactionId
     * @param transactionItem - it does not have to be owned by this particular node
     * @param consistencyLevel
     * @return map with single entry
     */
    Map<TransactionItem, List<PartitionUpdate>> readSingleTransactionItem(TransactionId transactionId,
                                                                                TransactionItem transactionItem,
                                                                                ConsistencyLevel consistencyLevel);
}
