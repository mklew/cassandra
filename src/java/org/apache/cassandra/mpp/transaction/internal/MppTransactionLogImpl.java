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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.mpp.transaction.MppTransactionLog;
import org.apache.cassandra.mpp.transaction.TransactionId;

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

    Set<TransactionId> committed = ConcurrentHashMap.newKeySet();

    Set<TransactionId> rolledBack = ConcurrentHashMap.newKeySet();

    public void appendCommitted(TransactionId transactionId)
    {
        Optional<TxLog> txLog = checkTransactionInLog(transactionId);
        if(txLog.isPresent()) {
            logger.warn("appendCommitted RepeatedCommitToLog TxId {} was already added to log as {}", transactionId, txLog.get());
        }
        else
        {
            committed.add(transactionId);
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
            rolledBack.add(transactionId);
        }
    }

    public Optional<TxLog> checkTransactionInLog(TransactionId transactionId)
    {
        boolean inCommitted = this.committed.contains(transactionId);
        boolean rolledBack = this.rolledBack.contains(transactionId);

        if(inCommitted && rolledBack) {
            String msg = "Transaction with TxId " + transactionId + " was in committed log and in rolled back log. Something is very bad";
            logger.error(msg + " Will throw runtime exception with same message");
            throw new RuntimeException(msg);
        }

        if(inCommitted)
            return Optional.of(TxLog.COMMITTED);
        else if(rolledBack)
            return Optional.of(TxLog.ROLLED_BACK);
        else return Optional.empty();
    }
}
