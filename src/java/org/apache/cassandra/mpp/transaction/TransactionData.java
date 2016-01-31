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

import java.util.Collection;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;

/**
 * Stores {@link org.apache.cassandra.mpp.transaction.PrivateMemtable} for that transaction.
 *
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 01/11/15
 */
public interface TransactionData extends ReadableTransactionData
{
    TransactionId getTxId();

    /**
     * @param ttl in ms
     * @return
     */
    boolean isExpired(int ttl);

    void addMutation(Mutation mutation);

    Collection<String> modifiedCfs();

    Collection<TransactionItem> asTransactionItems();

    Stream<PartitionUpdate> partitionUpdatesStream(String ksName, UUID cfId);

    void applyAllMutations(long applyTimestamp);
}
