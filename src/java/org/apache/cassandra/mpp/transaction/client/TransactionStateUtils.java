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

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

import org.apache.cassandra.SystemClock;
import org.apache.cassandra.utils.UUIDGen;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 26/11/15
 */
public class TransactionStateUtils
{

    /**
     * Warning: should be called from inside C*, because of time synchronization between nodes.
     *
     * @return new empty {@link TransactionState} with ID assigned
     */
    public static TransactionState newTransactionState()
    {
        return new TransactionState(UUIDGen.getTimeUUID(SystemClock.getCurrentTimeMillis()), Collections.emptyList());
    }

    public static TransactionState recreateTransactionState(UUID id, Collection<TransactionItem> transactionItems)
    {
        return new TransactionState(id, transactionItems);
    }
}
