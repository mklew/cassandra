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
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.mpp.MppServicesLocator;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 31/01/16
 */
public class CommitTransactionStatement implements CQLStatement
{

    private final int boundTermsSize;
    private final Term transactionStateAsJson;

    public CommitTransactionStatement(int boundTermsSize, Term transactionStateAsJson)
    {
        this.boundTermsSize = boundTermsSize;
        this.transactionStateAsJson = transactionStateAsJson;
    }

    public int getBoundTerms()
    {
        return boundTermsSize;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {

    }

    public void validate(ClientState state) throws RequestValidationException
    {
        checkFalse(transactionStateAsJson == null, "Transaction state should be present");
    }

    public ResultMessage execute(QueryState state, QueryOptions options) throws RequestValidationException, RequestExecutionException
    {
        return executeCommitTransactionStatement(options);
    }

    private ResultMessage executeCommitTransactionStatement(QueryOptions options)
    {
        TransactionState transactionState = MppStatementUtils.getTransactionState(options, transactionStateAsJson);
        System.out.println("CommitTransactionStatement transactionState is " + transactionState);

        MppServicesLocator.getInstance().commitTransaction(transactionState, options.getConsistency());
        return null;
    }

    public ResultMessage executeInternal(QueryState state, QueryOptions options) throws RequestValidationException, RequestExecutionException
    {
        return executeCommitTransactionStatement(options);
    }

    public Iterable<Function> getFunctions()
    {
        // TODO [MPP] maybe do something about it
        return Collections.emptyList();
    }

    public static class Parsed extends ParsedStatementWithTransaction
    {

        final Term.Raw transactionStateJson;

        public Parsed(Term.Raw transactionStateJson)
        {
            this.transactionStateJson = transactionStateJson;
        }

        public Prepared prepare() throws RequestValidationException
        {
            Term transactionStateAsJson = this.transactionStateJson != null ? prepareTransactionStateAsJson(this.transactionStateJson) : null;

            CommitTransactionStatement stmt = new CommitTransactionStatement(getBoundVariables().size(), transactionStateAsJson);

            return new Prepared(stmt, getBoundVariables(), null);
        }
    }
}
