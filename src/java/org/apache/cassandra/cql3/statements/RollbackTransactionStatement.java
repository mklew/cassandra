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

import java.nio.ByteBuffer;
import java.util.UUID;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.mpp.MppServicesLocator;
import org.apache.cassandra.mpp.transaction.MppServiceUtils;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.mpp.transaction.client.TransactionStateUtils;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 24/01/16
 */
public class RollbackTransactionStatement implements CQLStatement
{
    private final Term transactionId;

    private final boolean isLocal;

    private final int boundTerms;

    public RollbackTransactionStatement(Term transactionId, boolean isLocal, int boundTerms)
    {
        this.transactionId = transactionId;
        this.isLocal = isLocal;
        this.boundTerms = boundTerms;
    }

    public int getBoundTerms()
    {
        return boundTerms;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {

    }

    public void validate(ClientState state) throws RequestValidationException
    {

    }

    private UUID getTransactionId(QueryOptions options)
    {
        ByteBuffer b = checkNotNull(transactionId.bindAndGet(options), "Invalid null value of transaction id");
        UUIDType.instance.validate(b);
        final UUID transactionId = UUIDType.instance.compose(b);
        return transactionId;
    }

    public ResultMessage execute(QueryState state, QueryOptions options) throws RequestValidationException, RequestExecutionException
    {
        throw new RuntimeException("RollbackTransactionStatment execute has not been implemented");
    }

    public ResultMessage executeInternal(QueryState state, QueryOptions options) throws RequestValidationException, RequestExecutionException
    {
        final UUID transactionId1 = getTransactionId(options);

        final TransactionState transactionState = TransactionStateUtils.fromId(transactionId1);

        if (isLocal)
        {
            MppServicesLocator.getInstance().rollbackTransactionLocal(transactionState);
            return null;
        }
        else
        {
            throw new RuntimeException("This has not yet been implemented because it requries transaction items passed in");
        }
    }

    public Iterable<Function> getFunctions()
    {
        return transactionId.getFunctions();
    }


    public static class Parsed extends ParsedStatement
    {

        public final Term.Raw transactionId;
        public final boolean isLocal;

        public Parsed(Term.Raw transactionId, boolean isLocal)
        {
            this.transactionId = transactionId;
            this.isLocal = isLocal;
        }

        public Prepared prepare() throws RequestValidationException
        {
            Term transactionId = prepareTransactionId(this.transactionId);

            final RollbackTransactionStatement stmt = new RollbackTransactionStatement(transactionId, isLocal, getBoundVariables().size());
            return new ParsedStatement.Prepared(stmt, getBoundVariables(), null);
        }

        private Term prepareTransactionId(Term.Raw transactionId)
        {
            return transactionId.prepare(MppServiceUtils.KS_NAME, transactionIdReceiver());
        }

        private ColumnSpecification transactionIdReceiver()
        {
            return new ColumnSpecification(MppServiceUtils.KS_NAME, MppServiceUtils.TRANSACTION_STATE_CF_NAME, new ColumnIdentifier("[transaction_id]", true), UUIDType.instance);
        }
    }
}
