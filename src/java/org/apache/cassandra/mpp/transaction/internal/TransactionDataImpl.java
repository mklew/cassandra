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

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.mpp.transaction.TransactionData;
import org.apache.cassandra.mpp.transaction.TransactionId;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 01/11/15
 */
public class TransactionDataImpl implements TransactionData
{
    private final TransactionId txId;
    private final long creationNano = System.nanoTime();

    // TODO this will be changed to actual private memtables
    private Collection<Mutation> mutations = new ArrayList<>();

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
        mutations.add(mutation);
    }

    public Collection<String> modifiedCfs()
    {
        return mutations.stream().flatMap(m -> m.getPartitionUpdates().stream().map(x -> x.metadata().cfName)).collect(Collectors.toSet());
    }
}
