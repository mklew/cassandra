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
import java.util.UUID;
import java.util.function.Consumer;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.mpp.MppServicesLocator;
import org.apache.cassandra.mpp.transaction.TransactionTimeUUID;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.mpp.transaction.MppServiceUtils.mapTransactionStateToResultSet;
import static org.apache.cassandra.mpp.transaction.MppServiceUtils.transformResultSetToResultMessage;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 24/01/16
 */
public class ReadTransactionStatement implements CQLStatement
{
    private final boolean isLocal;
    private final boolean isJson;
    private final Term transactionId;
    private final CFName cfName;
    private final Term preparedToken;
    private final int boundTermsSize;
    private final Term transactionStateAsJson;

    public ReadTransactionStatement(boolean isLocal, boolean isJson, Term transactionId, CFName cfName, Term preparedToken, int boundTermsSize, Term transactionStateAsJson)
    {
        this.isLocal = isLocal;
        this.isJson = isJson;
        this.transactionId = transactionId;
        this.cfName = cfName;
        this.preparedToken = preparedToken;
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
        checkFalse(isLocal() && transactionId == null, "If read is locally then transaction id is required");
        checkFalse(isLocal() && isJson() && transactionId != null && cfName != null, "Returning rows in json form is not supported operation");
        checkFalse(!isLocal() && transactionStateAsJson == null, "If read is not local then transaction state as json has to be available");
    }

    public boolean isLocal()
    {
        return isLocal;
    }

    public boolean isJson()
    {
        return isJson;
    }

    public ResultMessage execute(QueryState state, QueryOptions options) throws RequestValidationException, RequestExecutionException
    {
        return executeReadTransactionStatement(options);
    }

    private ResultMessage executeReadTransactionStatement(QueryOptions options)
    {
        // TODO [MPP] Implement it.
        if(isLocal()) {
            assert transactionId != null;
            final UUID transactionId = MppStatementUtils.getTransactionId(options, this.transactionId);
            final TransactionTimeUUID txId = new TransactionTimeUUID(transactionId);


            if(isReadingJustTransactionState()) {
                // Returns local TransactionState for this transaction,
                TransactionState txState = MppServicesLocator.getInstance().readLocalTransactionState(txId);

                return transformResultSetToResultMessage(mapTransactionStateToResultSet(txState, isJson));
            }

            final ResultMessage[] message = new ResultMessage[1];
            final Consumer<PartitionIterator> cb = partitionIterator -> {
                // Idea is to extend SelectStatement and just execute it in order not to duplicate a lot of logic.
                // Tricky part is setting everything on the SelectStatement. For now, fail with exception

                CFMetaData metaData = Schema.instance.getCFMetaData(cfName.getKeyspace(), cfName.getColumnFamily());
                final ResultMessage resultMessage = MppFakeSelect.create(metaData).createResultMessage(partitionIterator);
                message[0] = resultMessage;
            };
            if (isReadingSpecificTokensFromColumnFamily()) {
                // read all from column family

                Murmur3Partitioner.LongToken token = MppStatementUtils.getToken(options, this.preparedToken);

                MppServicesLocator
                .getInstance()
                .readAllByColumnFamilyAndToken(txId,
                                               cfName.getKeyspace(),
                                               cfName.getColumnFamily(),
                                               token,
                                               cb);

                return message[0];
            }
            else if (isReadingWholeColumnFamily())
            {
                // read all from column family
                MppServicesLocator
                .getInstance()
                .readAllByColumnFamily(txId,
                                       cfName.getKeyspace(),
                                       cfName.getColumnFamily(),
                                       cb);

                return message[0];
            }
            else {
                throw new RuntimeException("ReadTransactionStatement. other case not implemented");
            }
        }
        else {
            assert transactionStateAsJson != null;

            TransactionState transactionState = MppStatementUtils.getTransactionState(options, this.transactionStateAsJson);


            if(isReadingSpecificTokensFromColumnFamily()) {
                throw new RuntimeException("ReadTransactionStatement reading quorum by CF && TOKEN not implemented");
            }

            if(isReadingWholeColumnFamily()) {
                throw new RuntimeException("ReadTransactionStatement reading quorum by CF not implemented");
            }

            throw new RuntimeException("ReadTransactionStatement illegal state");
        }
    }

    private boolean isReadingWholeColumnFamily()
    {
        return cfName != null;
    }

    private boolean isReadingSpecificTokensFromColumnFamily()
    {
        return cfName != null && preparedToken != null;
    }

    private boolean isReadingJustTransactionState()
    {
        return cfName == null && preparedToken == null;
    }

    public ResultMessage executeInternal(QueryState state, QueryOptions options) throws RequestValidationException, RequestExecutionException
    {
        return executeReadTransactionStatement(options);
    }

    public Iterable<Function> getFunctions()
    {
        // TODO [MPP] maybe do something about it
        return Collections.emptyList();
    }

    public static class Parsed extends ParsedStatementWithTransaction {

        final boolean isLocal;
        final boolean isJson;
        final Term.Raw transactionId;
        final CFName cfName;
        final Term.Raw token;
        final Term.Raw transactionStateJson;

        public Parsed(boolean isLocal, boolean isJson, Term.Raw transactionId, CFName cfName, Term.Raw token, Term.Raw transactionStateJson)
        {
            this.isLocal = isLocal;
            this.isJson = isJson;
            this.transactionId = transactionId;
            this.cfName = cfName;
            this.token = token;
            this.transactionStateJson = transactionStateJson;
        }

        public Prepared prepare() throws RequestValidationException
        {
            Term transactionId = this.transactionId != null ? prepareTransactionId(this.transactionId) : null;
            Term preparedToken = this.token != null ? prepareToken(this.token) : null;
            Term transactionStateAsJson = this.transactionStateJson != null ? prepareTransactionStateAsJson(this.transactionStateJson) : null;

            ReadTransactionStatement stmt = new ReadTransactionStatement(isLocal, isJson, transactionId, cfName, preparedToken, getBoundVariables().size(), transactionStateAsJson);

            return new ParsedStatement.Prepared(stmt, getBoundVariables(), null);
        }


    }
}
