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

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.mpp.transaction.MppTransactionLog;
import org.apache.cassandra.mpp.transaction.TransactionId;
import org.apache.cassandra.mpp.transaction.TxLog;

import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.db.SystemKeyspace.MPP_TRANSACTION_LOG;

/**
 * this is trivial implementation based on in memory maps.
 * Could be moved to system table just like PaxosState
 *
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 22/04/16
 */
public class MppTransactionLogImpl implements MppTransactionLog
{
    private static final Logger logger = LoggerFactory.getLogger(MppTransactionLogImpl.class);

    public TxLog checkTransaction(TransactionId transactionId) {
        String req = "SELECT * FROM system.%s WHERE transaction_id = ?";
        UntypedResultSet results = executeInternal(String.format(req, MPP_TRANSACTION_LOG), transactionId.unwrap());
        if(results.isEmpty()) {
            return TxLog.UNKNOWN;
        }
        else {
            boolean isCommitted = results.one().getBoolean("committed");
            if(isCommitted) {
                return TxLog.COMMITTED;
            }
            else return TxLog.ROLLED_BACK;
        }
    }

    private void insert(TransactionId transactionId, boolean committed) {
        String req = "INSERT INTO system.%s (transaction_id, committed) VALUES (?,?)";
        executeInternal(String.format(req, MPP_TRANSACTION_LOG), transactionId.unwrap(), committed);
    }

    public void appendCommitted(TransactionId transactionId)
    {
        Optional<TxLog> txLog = checkTransactionInLog(transactionId);
        if(txLog.isPresent()) {
            logger.warn("appendCommitted RepeatedCommitToLog TxId {} was already added to log as {}", transactionId, txLog.get());
        }
        else
        {
            insert(transactionId, true);
        }
    }

    public void appendRolledBack(TransactionId transactionId)
    {
        Optional<TxLog> txLog = checkTransactionInLog(transactionId);
        if(txLog.isPresent()) {
            logger.warn("appendRolledBack RepeatedCommitToLog TxId {} was already added to log as {}", transactionId, txLog.get());
        }
        else
        {
            insert(transactionId, false);
        }
    }

    public Optional<TxLog> checkTransactionInLog(TransactionId transactionId)
    {
        TxLog txLog = checkTransaction(transactionId);
        if(txLog == TxLog.UNKNOWN) {
            return Optional.empty();
        }
        else
        {
            return Optional.of(txLog);
        }
    }
}
