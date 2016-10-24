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

import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import org.apache.cassandra.SystemClock;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.mpp.transaction.MppCQLTester;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.mpp.transaction.MppTestingUtilities.newTransactionItem;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 04/04/16
 */
public class CommitTransactionStatementTest extends MppCQLTester
{

    String ksName = keyspace();
    String cfName = createTableName();
    long token1 = 1;
    long token2 = 2;
    long token3 = 3;
    long token4 = 4;

    final TransactionItem ti1 = newTransactionItem(ksName, cfName, token1);
    final TransactionItem ti2 = newTransactionItem(ksName, cfName, token2);
    final TransactionItem ti3 = newTransactionItem(ksName, cfName, token3);
    final TransactionItem ti4 = newTransactionItem(ksName, cfName, token4);

    private final AtomicLong txStateCount = new AtomicLong(0);
    private final long startingTime = SystemClock.getCurrentTimeMillis();
    private TransactionState newTransactionState(TransactionItem... items) {
        long timestampForThatTx = txStateCount.incrementAndGet() + startingTime;

        TransactionState transactionState = new TransactionState(UUIDGen.getTimeUUID(timestampForThatTx), Collections.emptyList());
        for (TransactionItem item : items)
        {
            transactionState.addTxItem(item);
        }
        return transactionState;
    }
    @Test
    public void shouldAcceptCommitTransactionTest() {

        ClientState state = ClientState.forInternalCalls();
        QueryState queryState = new QueryState(state);
        String stmt = "COMMIT TRANSACTION AS JSON ?";

        ParsedStatement.Prepared prepared = QueryProcessor.parseStatement(stmt, queryState);
        prepared.statement.validate(state);

//        final UUID txId = UUIDs.timeBased();
//
//        final ByteBuffer idBb = UUIDType.instance.decompose(txId);
//        TransactionState transactionState = newTransactionState(ti1, ti2, ti3);
//        String transactionStateAsJson = MppServiceUtils.getTransactionStateAsJson(transactionState);
//        ByteBuffer decompose = UTF8Type.instance.decompose(transactionStateAsJson);
//        QueryOptions options = QueryOptions.forInternalCalls(Collections.singletonList(decompose));
//
//        ResultMessage message = prepared.statement.executeInternal(queryState, options);
//        ResultMessage.Rows rows = (ResultMessage.Rows) message;
//        Assert.assertEquals("should have 3 rows", 3, rows.result.size());
    }

    @Test
    public void shouldParseCommitTransactionWithIfExists() {
        ClientState state = ClientState.forInternalCalls();
        QueryState queryState = new QueryState(state);
        String keyspace = keyspace();
        String table = createTable("CREATE TABLE %s (k int PRIMARY KEY, s text, i int)");
        String stmt = "COMMIT TRANSACTION AS JSON ? IF " + keyspace + "." + table + " WHERE k = 10 EXISTS";

        ParsedStatement.Prepared prepared = QueryProcessor.parseStatement(stmt, queryState);
        prepared.statement.validate(state);
    }

    @Test
    public void shouldParseCommitTransactionWithIfNotExists() {
        ClientState state = ClientState.forInternalCalls();
        QueryState queryState = new QueryState(state);
        String keyspace = keyspace();
        String table = createTable("CREATE TABLE %s (k int PRIMARY KEY, s text, i int)");
        String stmt = "COMMIT TRANSACTION AS JSON ? IF " + keyspace + "." + table + " WHERE k = 10 NOT EXISTS";

        ParsedStatement.Prepared prepared = QueryProcessor.parseStatement(stmt, queryState);
        prepared.statement.validate(state);
    }

    @Test
    public void shouldParseCommitTransactionWithIfCondition() {
        ClientState state = ClientState.forInternalCalls();
        QueryState queryState = new QueryState(state);
        String keyspace = keyspace();
        String table = createTable("CREATE TABLE %s (k int PRIMARY KEY, s text, i int)");
        String stmt = "COMMIT TRANSACTION AS JSON ? IF " + keyspace + "." + table + " WHERE k = 10 MATCHES s = 'asd'";

        ParsedStatement.Prepared prepared = QueryProcessor.parseStatement(stmt, queryState);
        prepared.statement.validate(state);
    }

    @Test
    public void shouldParseCommitTransactionWithTwoConditions() {
        ClientState state = ClientState.forInternalCalls();
        QueryState queryState = new QueryState(state);
        String keyspace = keyspace();
        String table = createTable("CREATE TABLE %s (k int PRIMARY KEY, s text, i int)");
        String stmt = "COMMIT TRANSACTION AS JSON ? IF " + keyspace + "." + table + " WHERE k = 10 MATCHES s = 'asd' AND i > 100";

        ParsedStatement.Prepared prepared = QueryProcessor.parseStatement(stmt, queryState);
        prepared.statement.validate(state);
    }

    @Test
    public void shouldParseCommitTransactionWithIfThreeConditions() {
        ClientState state = ClientState.forInternalCalls();
        QueryState queryState = new QueryState(state);
        String keyspace = keyspace();
        String table = createTable("CREATE TABLE %s (k int PRIMARY KEY, s text, i int)");
        String stmt = "COMMIT TRANSACTION AS JSON ? IF " + keyspace + "." + table + " WHERE k = 10 MATCHES s = 'asd' AND i > 100 AND i < 200";

        ParsedStatement.Prepared prepared = QueryProcessor.parseStatement(stmt, queryState);
        prepared.statement.validate(state);
    }

    @Test
    public void shouldParseCommitTransactionWithIfTwoConditionsAndTwoKeys() {
        ClientState state = ClientState.forInternalCalls();
        QueryState queryState = new QueryState(state);
        String keyspace = keyspace();
        String table = createTable("CREATE TABLE %s (k int, s text, i int, PRIMARY KEY (k,s)) ");
        String stmt = "COMMIT TRANSACTION AS JSON ? IF " + keyspace + "." + table + " WHERE k = 10 and s = 'foo' MATCHES i > 100 AND i < 200";

        ParsedStatement.Prepared prepared = QueryProcessor.parseStatement(stmt, queryState);
        prepared.statement.validate(state);
    }


}
