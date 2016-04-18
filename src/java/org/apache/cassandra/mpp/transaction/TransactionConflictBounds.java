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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import com.datastax.driver.core.PublicTokenRangeFactory;
import com.datastax.driver.core.TokenRange;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;
import org.apache.cassandra.mpp.transaction.internal.TransactionConflictBoundsStrategy;
import org.apache.cassandra.serializers.UTF8Serializer;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 18/04/16
 */
public class TransactionConflictBounds
{
    public final static String CF_METADATA_EXTENSION_TRANSACTION_CONFLICT_BOUNDS = "transaction_conflict_bounds";

    public final static String CF_METADATA_EXTENSION_TOKEN_RANGE_SLICES = "token_range_slices";

    public static TransactionConflictBounds COMMON_TX_ITEMS_BOUNDS = new TransactionConflictBounds(TransactionConflictBoundsStrategy.COMMON_TX_ITEMS, Optional.<Integer>empty());

    public static TransactionConflictBounds ONE_FOR_ALL_BOUNDS = new TransactionConflictBounds(TransactionConflictBoundsStrategy.ONE_FOR_ALL, Optional.<Integer>empty());

    public TransactionConflictBounds(TransactionConflictBoundsStrategy strategy, Optional<Integer> slices)
    {
        Preconditions.checkArgument(strategy != TransactionConflictBoundsStrategy.TOKEN_RANGE_SLICES || (slices.isPresent()));
        this.strategy = strategy;
        this.slices = slices;
    }

    public MppIndexKey computeKey(TransactionItem transactionItem) {
        if(strategy == TransactionConflictBoundsStrategy.COMMON_TX_ITEMS) {
            return new MppIndexKey(transactionItem, getCfName(transactionItem));
        }
        else if (strategy == TransactionConflictBoundsStrategy.ONE_FOR_ALL) {
            return new MppIndexKey(0, getCfName(transactionItem));
        }
        else if (strategy == TransactionConflictBoundsStrategy.TOKEN_RANGE_SLICES) {
            Integer slicesOfTokenRanges = slices.get();

            List<TokenRange> tokenRanges = PublicTokenRangeFactory.getFullMurmur3TokenRange().splitEvenly(slicesOfTokenRanges);
            TokenRange tokenRange = tokenRanges.stream().filter(tr -> tr.contains(PublicTokenRangeFactory.convert(transactionItem.getToken()))).findFirst().get();
            int tokenRangeSlice = tokenRanges.indexOf(tokenRange);
            return new MppIndexKey(tokenRangeSlice, getCfName(transactionItem));
        }
        else {
            throw new RuntimeException("New TransactionConflictStrategy that is not handled. TODO, handle strategy " + strategy);
        }
    }

    private String getCfName(TransactionItem transactionItem)
    {
        return transactionItem.getKsName()+"."+transactionItem.getCfName();
    }

    private final TransactionConflictBoundsStrategy strategy;

    private final Optional<Integer> slices;

    public static MppIndexKey findIndexKey(String ksName, String cfName, TransactionItem transactionItem) {
        TransactionConflictBounds conflictBoundsStrategy = findConflictBoundsStrategy(ksName, cfName);
        return conflictBoundsStrategy.computeKey(transactionItem);
    }

    public static TransactionConflictBounds findConflictBoundsStrategy(String ksName, String cfName) {
        CFMetaData cfMetaData = Keyspace.open(ksName).getColumnFamilyStore(cfName).metadata;

        ImmutableMap<String, ByteBuffer> extensions = cfMetaData.params.extensions;

        TransactionConflictBoundsStrategy strategy = findStrategy(extensions);

        if(strategy == TransactionConflictBoundsStrategy.TOKEN_RANGE_SLICES) {
            // find token range slices parameter
            String slicesAsString = UTF8Serializer.instance.deserialize(extensions.get(CF_METADATA_EXTENSION_TOKEN_RANGE_SLICES));
            int slices = Integer.parseInt(slicesAsString);
            return new TransactionConflictBounds(strategy, Optional.of(slices));
        }
        else
        {
            return new TransactionConflictBounds(strategy, Optional.empty());
        }
    }

    private static TransactionConflictBoundsStrategy findStrategy(ImmutableMap<String, ByteBuffer> extensions)
    {
        if(extensions.containsKey(CF_METADATA_EXTENSION_TRANSACTION_CONFLICT_BOUNDS)) {
            String strategyStr = UTF8Serializer.instance.deserialize(extensions.get(CF_METADATA_EXTENSION_TRANSACTION_CONFLICT_BOUNDS));
            return TransactionConflictBoundsStrategy.valueOf(strategyStr);
        }
        else return TransactionConflictBoundsStrategy.COMMON_TX_ITEMS; // default
    }
}
