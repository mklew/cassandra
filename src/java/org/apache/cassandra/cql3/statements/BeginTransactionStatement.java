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

package org.apache.cassandra.cql3.statements;

import java.util.Collections;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.mpp.MppServicesLocator;
import org.apache.cassandra.mpp.transaction.MppServiceUtils;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 24/01/16
 */
public class BeginTransactionStatement extends ParsedStatement implements CQLStatement
{
    public int getBoundTerms()
    {
        return 0;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {

    }

    public void validate(ClientState state) throws RequestValidationException
    {

    }

    public ResultMessage execute(QueryState state, QueryOptions options) throws RequestValidationException, RequestExecutionException
    {
        return executeBeginStatement();
    }

    public ResultMessage executeInternal(QueryState state, QueryOptions options) throws RequestValidationException, RequestExecutionException
    {
        return executeBeginStatement();
    }

    public static ResultMessage executeBeginStatement()
    {
        final TransactionState transactionState = MppServicesLocator.getInstance().beginTransaction();
        ResultSet resultSet = MppServiceUtils.mapTransactionStateToResultSet(transactionState);
        return MppServiceUtils.transformResultSetToResultMessage(resultSet);
    }

    public Prepared prepare() throws RequestValidationException
    {
        return new Prepared(this);
    }

    public Iterable<Function> getFunctions()
    {
        return Collections.emptyList();
    }
}
