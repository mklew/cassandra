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

import org.apache.cassandra.mpp.transaction.TransactionId;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;
import org.apache.cassandra.mpp.transaction.client.TransactionStateUtils;
import org.apache.cassandra.mpp.transaction.network.messages.PrivateMemtableReadCommand;

/**
 * It has info of what should be queried - {@link TransactionItem}
 * <p>
 * and who {@link SingleReadRequestRecipe#getReceipients()}
 * <p>
 * and how many non empty replies with PartitionUpdate are required (because it knows replication factor)
 *
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 30/01/16
 */
public class SingleReadRequestRecipe
{
    final private TransactionId transactionId;

    final private int replicationFactor;

    final private TransactionItem transactionItem;

    final private List<InetAddress> receipients;

    public SingleReadRequestRecipe(TransactionId transactionId, TransactionItem transactionItem, int replicationFactor, List<InetAddress> receipients)
    {
        this.transactionId = transactionId;
        this.replicationFactor = replicationFactor;
        this.transactionItem = transactionItem;
        this.receipients = receipients;
    }

    public PrivateMemtableReadCommand createCommand()
    {
        return new PrivateMemtableReadCommand(TransactionStateUtils.recreateTransactionState(transactionId, Collections.singletonList(transactionItem)));
    }

    public List<InetAddress> getReceipients()
    {
        return receipients;
    }

    public TransactionItem getTransactionItem()
    {
        return transactionItem;
    }

    private static int quorumFor(int replicationFactor) {
        return (replicationFactor / 2 ) + 1;
    }

    public <T> boolean isQuorum(Collection<T> partitionUpdates)
    {
        return partitionUpdates.size() >= quorumFor(replicationFactor);
    }
}
