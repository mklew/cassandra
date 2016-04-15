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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.TransactionRolledBackException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.mpp.MppKeyspace;
import org.apache.cassandra.mpp.MppServicesLocator;
import org.apache.cassandra.mpp.transaction.MppServiceUtils;
import org.apache.cassandra.mpp.transaction.TransactionId;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 31/01/16
 */
public class CommitTransactionStatement implements CQLStatement
{

    private final int boundTermsSize;
    private final Term transactionStateAsJson;

    private static final ColumnIdentifier TX_RESULT_COLUMN = new ColumnIdentifier("[committed]", false);

    private static final ColumnIdentifier TX_ID_RESULT_COLUMN = new ColumnIdentifier("[tx_id]", false);

    private static final Logger logger = LoggerFactory.getLogger(CommitTransactionStatement.class);

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
//        checkFalse(transactionStateAsJson == null, "Transaction state should be present");
    }

    public ResultMessage execute(QueryState state, QueryOptions options) throws RequestValidationException, RequestExecutionException
    {
        return executeCommitTransactionStatement(options, state.getClientState());
    }

    private ResultMessage executeCommitTransactionStatement(QueryOptions options, ClientState clientState)
    {
        TransactionState transactionState = MppStatementUtils.getTransactionState(options, transactionStateAsJson);


        System.out.println("CommitTransactionStatement transactionState is " + transactionState);

        try {
            boolean committed = MppServicesLocator.getInstance().commitTransaction(transactionState, options.getConsistency(), options, clientState);
            return MppServiceUtils.transformResultSetToResultMessage(buildTxResultSet(committed, transactionState.id()));
        }
        catch (TransactionRolledBackException rolledBack)
        {
            logger.warn("Transaction was rolled back " + rolledBack.getMessage(), rolledBack);
            return MppServiceUtils.transformResultSetToResultMessage(buildTxResultSet(false, rolledBack.getRolledBackTransaction().id()));
        }
        catch (Throwable e) {
            logger.error("Error occurred during CommitTransaction. TxID {}", transactionState.getTransactionId(), e);
            throw e;
        }
    }

    public static ResultSet buildTxResultSet(boolean success, TransactionId txId)
    throws InvalidRequestException
    {
        ColumnSpecification spec = new ColumnSpecification(MppKeyspace.NAME, MppKeyspace.TABLE_NAME_FOR_RESULT_SET, TX_RESULT_COLUMN, BooleanType.instance);
        ColumnSpecification spec2 = new ColumnSpecification(MppKeyspace.NAME, MppKeyspace.TABLE_NAME_FOR_RESULT_SET, TX_ID_RESULT_COLUMN, UUIDType.instance);
        ResultSet.ResultMetadata metadata = new ResultSet.ResultMetadata(Arrays.asList(spec, spec2));
        List<List<ByteBuffer>> rows = Collections.singletonList(Arrays.asList(BooleanType.instance.decompose(success), UUIDType.instance.decompose(txId.unwrap())));

        ResultSet rs = new ResultSet(metadata, rows);
        return rs;
    }

    public ResultMessage executeInternal(QueryState state, QueryOptions options) throws RequestValidationException, RequestExecutionException
    {
        return executeCommitTransactionStatement(options, state.getClientState());
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
