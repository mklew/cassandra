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
import java.util.List;

import org.apache.cassandra.mpp.transaction.client.TransactionItem;

/**
* @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
* @since 04/04/16
*/
public class TransactionItemWithAddresses implements WithEndpoints
{
    private final TransactionItem txItem;

    private final List<InetAddress> endPoints;

    private final Integer replicationFactor;

    TransactionItemWithAddresses(TransactionItem txItem, List<InetAddress> endPoints, int replicationFactor)
    {
        this.txItem = txItem;
        this.endPoints = endPoints;
        this.replicationFactor = replicationFactor;
    }

    public TransactionItem getTxItem()
    {
        return txItem;
    }

    public List<InetAddress> getEndPoints()
    {
        return endPoints;
    }

    public Integer getReplicationFactor()
    {
        return replicationFactor;
    }
}
