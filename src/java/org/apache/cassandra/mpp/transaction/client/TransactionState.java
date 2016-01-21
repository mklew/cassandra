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
import java.util.Set;
import java.util.UUID;


/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 26/11/15
 */
// TODO [MPP] Remove Serializable, run "shouldBeginNewTransaction test" and change it so that reason why it fails is visible in console.
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
}
