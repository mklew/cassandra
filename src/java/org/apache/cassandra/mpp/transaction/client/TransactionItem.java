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

import org.apache.cassandra.dht.Token;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 26/11/15
 */
public class TransactionItem
{
    /**
     * Probably {@link org.apache.cassandra.dht.Murmur3Partitioner.LongToken}
     */
    private final Token token;

    /**
     *
     */
    private final String ksName;

    /**
     *
     */
    private final String cfName;

    public TransactionItem(Token token, String ksName, String cfName)
    {
        this.token = token;
        this.ksName = ksName;
        this.cfName = cfName;
    }

    public Token getToken()
    {
        return token;
    }

    public String getKsName()
    {
        return ksName;
    }

    public String getCfName()
    {
        return cfName;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TransactionItem that = (TransactionItem) o;

        if (!cfName.equals(that.cfName)) return false;
        if (!ksName.equals(that.ksName)) return false;
        if (!token.equals(that.token)) return false;

        return true;
    }

    public int hashCode()
    {
        int result = token.hashCode();
        result = 31 * result + ksName.hashCode();
        result = 31 * result + cfName.hashCode();
        return result;
    }

    public String toString()
    {
        return "TransactionItem{" +
               "token=" + token +
               ", ksName='" + ksName + '\'' +
               ", cfName='" + cfName + '\'' +
               '}';
    }
}
