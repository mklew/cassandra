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

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 22/04/16
 */
public interface MppTransactionLog
{

    void appendCommitted(TransactionId transactionId);

    void appendRolledBack(TransactionId transactionId);

    Optional<TxLog> checkTransactionInLog(TransactionId transactionId);

    default boolean existsInLog(TransactionId transactionId) {
        return checkTransactionInLog(transactionId).isPresent();
    }

    default boolean txWasRolledBack(TransactionId transactionId) {
        return compareTxLog(transactionId, TxLog.ROLLED_BACK);
    }

    default boolean compareTxLog(TransactionId transactionId, TxLog expected)
    {
        Optional<TxLog> txLog = checkTransactionInLog(transactionId);
        return txLog.isPresent() && (txLog.get() == expected);
    }

    default boolean txWasCommitted(TransactionId transactionId) {
        return compareTxLog(transactionId, TxLog.COMMITTED);
    }
}
