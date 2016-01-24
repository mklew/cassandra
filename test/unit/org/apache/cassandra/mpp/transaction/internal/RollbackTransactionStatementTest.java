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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.UUID;

import org.junit.Test;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.mpp.transaction.MppCQLTester;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 24/01/16
 */
public class RollbackTransactionStatementTest extends MppCQLTester
{
    @Test
    public void shouldRollbackTransaction() throws Throwable
    {
        final UUID txId = UUIDs.timeBased();

        execute("ROLLBACK TRANSACTION LOCALLY " + txId);

        // TODO more assertions, hooks, maybe expose private memtable storage and others for testing.
    }

    @Test
    public void shouldWorkWithPreparedStatement() throws Throwable
    {
        ClientState state = ClientState.forInternalCalls();
        QueryState queryState = new QueryState(state);
        String stmt = "ROLLBACK TRANSACTION LOCALLY ?";

        ParsedStatement.Prepared prepared = QueryProcessor.parseStatement(stmt, queryState);
        prepared.statement.validate(state);

        final UUID txId = UUIDs.timeBased();

        final ByteBuffer idBb = UUIDType.instance.decompose(txId);

        QueryOptions options = QueryOptions.forInternalCalls(Collections.singletonList(idBb));

        final ResultMessage resultMessage = prepared.statement.executeInternal(queryState, options);
    }

}
