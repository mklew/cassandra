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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.mpp.transaction.MppService;
import org.apache.cassandra.mpp.transaction.TransactionId;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;
import org.apache.cassandra.mpp.transaction.client.TransactionState;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 20/01/16
 */
public class MppServiceImpl implements MppService
{
    private static final Logger logger = LoggerFactory.getLogger(MppServiceImpl.class);

    public TransactionState beginTransaction()
    {
        final UUID txId = UUIDs.timeBased();
        logger.info("Begin transaction id {}", txId);
        return new TransactionState(txId, Collections.emptyList());
    }

    public void commitTransaction()
    {
        // TODO [MPP] Implement it
        throw new NotImplementedException();
    }

    public void rollbackTransaction(TransactionState transactionState)
    {
        // TODO [MPP] Implement it
        throw new NotImplementedException();
    }

    public Map<TransactionItem, List<PartitionUpdate>> readTransactionDataLocalOnly(TransactionId transactionId)
    {
        // TODO [MPP] Implement it
        throw new NotImplementedException();
    }

    public Map<TransactionItem, List<PartitionUpdate>> readAllTransactionData(TransactionState transactionState)
    {
        // TODO [MPP] Implement it
        throw new NotImplementedException();
    }

    public Collection<TransactionId> getInProgressTransactions()
    {
        return null;
    }
}
