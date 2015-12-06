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

package org.apache.cassandra.mpp.transaction.network.messages;


import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.mpp.transaction.NodeContext;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;
import org.apache.cassandra.mpp.transaction.network.MppRequestMessage;
import org.apache.cassandra.mpp.transaction.network.MppResponseMessage;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 06/12/15
 */
public class QuorumReadRequest implements MppRequestMessage
{
    private static Logger logger = LoggerFactory.getLogger(QuorumReadRequest.class);

    private final UUID transactionId;

    private final List<TransactionItem> transactionItems;

    public QuorumReadRequest(UUID transactionId, List<TransactionItem> transactionItems)
    {
        Preconditions.checkArgument(!transactionItems.isEmpty(), "Expected to have non empty list of transaction items");
        this.transactionId = transactionId;
        this.transactionItems = transactionItems;

    }

    public MppResponseMessage executeInLocalContext(NodeContext context)
    {
        final Map<TransactionItem, List<PartitionUpdate>> transactionItemToPartitionUpdates = context.getStorage().readTransactionItems(transactionId, transactionItems);

        final int numberOfMissingItems = numberOfMissingItems(transactionItemToPartitionUpdates);
        if(numberOfMissingItems != 0) {
            final Set<TransactionItem> missingItems = getMissingItems(transactionItemToPartitionUpdates);
            logger.warn("This node has {} missing transaction items {} for transaction id {}", numberOfMissingItems, missingItems, transactionId);
        }

        return new QuorumReadResponse(transactionId, transactionItemToPartitionUpdates, numberOfMissingItems != 0);
    }

    private int numberOfMissingItems(Map<TransactionItem, List<PartitionUpdate>> transactionItemToPartitionUpdates)
    {
        return transactionItemToPartitionUpdates.keySet().size() - transactionItems.size();
    }

    private Set<TransactionItem> getMissingItems(Map<TransactionItem, List<PartitionUpdate>> transactionItemToPartitionUpdates)
    {
        final Set<TransactionItem> missingItems = new HashSet<>(transactionItems);
        missingItems.removeAll(transactionItemToPartitionUpdates.keySet());
        return missingItems;
    }
}
