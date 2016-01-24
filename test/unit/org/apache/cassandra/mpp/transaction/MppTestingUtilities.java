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

import java.util.Collections;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.mpp.transaction.client.TransactionStateUtils;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 24/01/16
 */
public class MppTestingUtilities
{
    public static final String START_TRANSACTION = "START TRANSACTION";

    public static TransactionState mapResultToTransactionState(UntypedResultSet resultSet)
    {
        final Stream<UntypedResultSet.Row> stream = MppServiceUtils.streamResultSet(resultSet);
        return stream.map(ROW_TO_TRANSACTION_STATE).findFirst().get();
    }

    public final static Function<UntypedResultSet.Row, TransactionState> ROW_TO_TRANSACTION_STATE = row -> {
        final UUID transactionId = row.getUUID(MppServiceUtils.TRANSACTION_ID_NAME_COL);
//        final String cfName = row.getString(MppServiceUtils.CF_NAME_COL);
//        final long token = row.getLong(MppServiceUtils.TOKEN_NAME_COL);
        final TransactionState transactionState = TransactionStateUtils.recreateTransactionState(transactionId, Collections.emptyList());
        return transactionState;
    };
}
