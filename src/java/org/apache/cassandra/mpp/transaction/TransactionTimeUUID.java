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

import java.util.UUID;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 01/11/15
 */
public class TransactionTimeUUID implements TransactionId, Comparable<TransactionId>
{
    private final UUID id;

    public TransactionTimeUUID(UUID id)
    {
        this.id = id;
    }

    public int compareTo(TransactionId o)
    {
        return id.compareTo(((TransactionTimeUUID)o).id);
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TransactionTimeUUID that = (TransactionTimeUUID) o;

        if (!id.equals(that.id)) return false;

        return true;
    }

    public int hashCode()
    {
        return id.hashCode();
    }

    public String toString()
    {
        return "TxID[" + id + ']';
    }
}
