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

import java.util.Optional;

import org.apache.cassandra.mpp.transaction.client.TransactionItem;

/**
 * Data holder for all possible index access strategies.
 * Having it as common data structure solves issue of proper equals, hashcode.
 *
 * It is easier to understand than hierarchy of keys.
 *
 * Keys are defined per cfName hence cfName fields (which is full name: ks.cf)
 */
public class MppIndexKey
{
    private final String cfName;
    private final Optional<TransactionItem> transactionItem;
    private final Optional<Integer> tokenRangeSliceNumber;

    public MppIndexKey(TransactionItem transactionItem, String cfName)
    {
        this.cfName = cfName;
        this.transactionItem = Optional.of(transactionItem);
        this.tokenRangeSliceNumber = Optional.empty();
    }

    public MppIndexKey(Integer tokenRangeSliceNumber, String cfName)
    {
        this.cfName = cfName;
        this.tokenRangeSliceNumber = Optional.of(tokenRangeSliceNumber);
        this.transactionItem = Optional.empty();
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MppIndexKey that = (MppIndexKey) o;

        if (!cfName.equals(that.cfName)) return false;
        if (!tokenRangeSliceNumber.equals(that.tokenRangeSliceNumber)) return false;
        if (!transactionItem.equals(that.transactionItem)) return false;

        return true;
    }

    public int hashCode()
    {
        int result = cfName.hashCode();
        result = 31 * result + transactionItem.hashCode();
        result = 31 * result + tokenRangeSliceNumber.hashCode();
        return result;
    }
}
