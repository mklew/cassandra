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

package org.apache.cassandra.mpp.transaction.client;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Preconditions;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.mpp.transaction.TransactionId;
import org.apache.cassandra.mpp.transaction.TransactionTimeUUID;
import org.joda.time.DateTime;


/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 26/11/15
 */
public class TransactionState implements Serializable
{
    /**
     * TimeUUID
     */
    private final UUID transactionId;

    private final Collection<TransactionItem> transactionItems;

    public TransactionState(UUID transactionId, Collection<TransactionItem> transactionItems)
    {
        this.transactionId = transactionId;
        this.transactionItems = new HashSet<>(transactionItems);
    }

    public void addTxItem(TransactionItem item) {
        transactionItems.add(item);
    }

    public TransactionState merge(TransactionState other) {
        assert transactionId.equals(other.transactionId);
        final Set<TransactionItem> copy = new HashSet<>(transactionItems);
        copy.addAll(other.transactionItems);
        return new TransactionState(transactionId, copy);
    }

    public UUID getTransactionId()
    {
        return transactionId;
    }

    public TransactionId id() {
        return new TransactionTimeUUID(transactionId);
    }

    public Collection<TransactionItem> getTransactionItems()
    {
        return Collections.unmodifiableCollection(transactionItems);
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TransactionState that = (TransactionState) o;

        if (!transactionId.equals(that.transactionId)) return false;
        if (!transactionItems.equals(that.transactionItems)) return false;

        return true;
    }

    public int hashCode()
    {
        int result = transactionId.hashCode();
        result = 31 * result + transactionItems.hashCode();
        return result;
    }

    public String toString()
    {
        return "TransactionState{" +
               "transactionId=" + transactionId +
               ", transactionItems=" + transactionItems +
               '}';
    }

    /**
     * This is a nice hack to encode meaning "I want to read transaction" by creating UUID
     * like that:
     *  {@code UUID uuid = UUIDs.startOf(new DateTime().plusYears(1).plusDays(1).getMillis()); }
     *
     *  So timestamp in 1 year plus 1 day in the future. I choose these dates arbitrarly.
     *
     * @return
     */
    public boolean isReadTransaction()
    {
        long timestamp = UUIDs.unixTimestamp(transactionId);
        return new DateTime(timestamp).minusYears(1).isAfterNow();
    }

    public Murmur3Partitioner.LongToken singleToken()
    {
        Preconditions.checkState(getTransactionItems().size() == 1);
        return (Murmur3Partitioner.LongToken)getTransactionItems().iterator().next().getToken();
    }

    public Optional<TransactionItem> findTransactionItem(String ksName, String cfName, Token token)
    {
        return transactionItems.stream().filter(ti -> ti.getKsName().equals(ksName)
                                                      && ti.getCfName().equals(cfName)
                                                      && ti.getToken().equals(token)).findFirst();
    }

    public boolean hasExactlySameItems(TransactionState otherTxState)
    {
        return transactionItems.equals(otherTxState);
    }

    public boolean hasItem(TransactionItem transactionItem)
    {
        return transactionItems.contains(transactionItem);
    }

    public boolean isEmpty() {
        return transactionItems.isEmpty();
    }
}
