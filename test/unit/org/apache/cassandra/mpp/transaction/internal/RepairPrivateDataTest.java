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

import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.mpp.transaction.MppCQLTester;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;
import org.apache.cassandra.mpp.transaction.client.TransactionState;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 17/02/16
 */
public class RepairPrivateDataTest extends MppCQLTester
{

    @Test
    public void testIt() throws Throwable {
        TransactionState transactionState = startTransaction();

        createMppTestCf1();
        createMppTestCf2();

        int modifiedKey = 101234;
        int key2 = 123;
        final String expectedTextValue = "this is some text";

        transactionState = txInsertToCf1(transactionState, 1001, "clustering_pk", "some description", 210);
        transactionState = txInsertToCf2(transactionState, 1002, "other_ck", 210);

        TransactionItem transactionItemForCf2 = transactionState.getTransactionItems().stream().filter(ti -> ti.getCfName().equals(cf2Name)).findFirst().get();

        // TODO this should be done in node1. Node2, Node3 should keep their data.
        //getMppService().deleteSingleItem();

        UUID txId = transactionState.getTransactionId();

        final UntypedResultSet resultSet1 = execute("INSERT INTO %s (k, s, i) VALUES (?, ?, ?) USING TRANSACTION " + txId, modifiedKey, expectedTextValue, 10);

        final String cfName2 = createTable("CREATE TABLE %s (k int PRIMARY KEY, b int)");

        final UntypedResultSet resultSet2 = execute("INSERT INTO %s (k, b) VALUES (?, ?) USING TRANSACTION " + txId, key2, 12351);

        // TODO implement it
        //getMppService().makeTransactionDataConsistent();
    }
}
