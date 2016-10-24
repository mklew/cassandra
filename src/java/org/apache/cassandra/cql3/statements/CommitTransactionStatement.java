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
import java.util.NavigableSet;
import java.util.Optional;
import java.util.SortedSet;

import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnCondition;
import org.apache.cassandra.cql3.ColumnConditions;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.Conditions;
import org.apache.cassandra.cql3.Operation;
import org.apache.cassandra.cql3.Operations;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.WhereClause;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionColumns;
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
import org.apache.cassandra.mpp.transaction.TransactionCommitResult;
import org.apache.cassandra.mpp.transaction.TransactionId;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 31/01/16
 */
public class CommitTransactionStatement implements CQLStatement
{

    private final int boundTermsSize;
    private final Term transactionStateAsJson;
    private final Optional<CommitConditions> commitConditions;

    public boolean isConditional() {
        return commitConditions.isPresent();
    }

    public CQL3CasRequest getRequest(QueryState queryState, QueryOptions options) {
        assert commitConditions.isPresent();
        return commitConditions.get().makeCasRequest(queryState, options);
    }

    public static final String CUSTOM_EXPRESSIONS_NOT_ALLOWED =
    "Custom index expressions cannot be used in WHERE clauses for UPDATE or DELETE statements";

    static class CommitConditions {
        private final CFMetaData metadata;
        private final Conditions commitConditions;
        private StatementRestrictions restrictionsForCondition;

        private final Operations operations;

        private final PartitionColumns updatedColumns;

        private final PartitionColumns conditionColumns;

        private final PartitionColumns requiresRead;

        CommitConditions(CFMetaData metadata, StatementRestrictions restrictions, Conditions commitConditions, Operations operations)
        {
            this.metadata = metadata;
            this.restrictionsForCondition = restrictions;
            this.commitConditions = commitConditions;
            this.operations = operations;

            CFMetaData cfm = this.metadata;
            PartitionColumns.Builder conditionColumnsBuilder = PartitionColumns.builder();
            Iterable<ColumnDefinition> columns = commitConditions.getColumns();
            if (columns != null)
                conditionColumnsBuilder.addAll(columns);

            PartitionColumns.Builder updatedColumnsBuilder = PartitionColumns.builder();
            PartitionColumns.Builder requiresReadBuilder = PartitionColumns.builder();
            for (Operation operation : this.operations)
            {
                updatedColumnsBuilder.add(operation.column);
                // If the operation requires a read-before-write and we're doing a conditional read, we want to read
                // the affected column as part of the read-for-conditions paxos phase (see #7499).
                if (operation.requiresRead())
                {
                    conditionColumnsBuilder.add(operation.column);
                    requiresReadBuilder.add(operation.column);
                }
            }

            PartitionColumns modifiedColumns = updatedColumnsBuilder.build();
            // Compact tables have not row marker. So if we don't actually update any particular column,
            // this means that we're only updating the PK, which we allow if only those were declared in
            // the definition. In that case however, we do went to write the compactValueColumn (since again
            // we can't use a "row marker") so add it automatically.
            if (cfm.isCompactTable() && modifiedColumns.isEmpty() && updatesRegularRows())
                modifiedColumns = cfm.partitionColumns();

            this.updatedColumns = modifiedColumns;
            this.conditionColumns = conditionColumnsBuilder.build();
            this.requiresRead = requiresReadBuilder.build();
        }

        public CFMetaData getMetadata()
        {
            return metadata;
        }

        public Conditions getCommitConditions()
        {
            return commitConditions;
        }

        public StatementRestrictions getRestrictionsForCondition()
        {
            return restrictionsForCondition;
        }

        public List<ByteBuffer> buildPartitionKeyNames(QueryOptions options)
        throws InvalidRequestException
        {
            List<ByteBuffer> partitionKeys = restrictionsForCondition.getPartitionKeys(options);
            for (ByteBuffer key : partitionKeys)
                QueryProcessor.validateKey(key);

            return partitionKeys;
        }

        private CQL3CasRequest makeCasRequest(QueryState queryState, QueryOptions options)
        {
            CFMetaData cfm = getMetadata();
            StatementType type = StatementType.SELECT;
            List<ByteBuffer> keys = buildPartitionKeyNames(options);
            // We don't support IN for CAS operation so far
            checkFalse(keys.size() > 1,
                       "IN on the partition key is not supported with conditional %s",
                       type.isUpdate()? "updates" : "deletions");

            DecoratedKey key = cfm.decorateKey(keys.get(0));
            long now = options.getTimestamp(queryState);
            SortedSet<Clustering> clusterings = createClustering(options);

            checkFalse(clusterings.size() > 1,
                       "IN on the clustering key columns is not supported with conditional %s",
                       type.isUpdate()? "updates" : "deletions");

            Clustering clustering = Iterables.getOnlyElement(clusterings);

            CQL3CasRequest request = new CQL3CasRequest(cfm, key, false, conditionColumns, updatesRegularRows(), updatesStaticRow());

            addConditions(clustering, request, options);
//            request.addRowUpdate(clustering, this, options, now);

            return request;
        }

        public boolean updatesRegularRows()
        {
            // We're updating regular rows if all the clustering columns are provided.
            // Note that the only case where we're allowed not to provide clustering
            // columns is if we set some static columns, and in that case no clustering
            // columns should be given. So in practice, it's enough to check if we have
            // either the table has no clustering or if it has at least one of them set.
            return metadata.clusteringColumns().isEmpty() || restrictionsForCondition.hasClusteringColumnsRestriction();
        }

        // TODO [MPT] Simplified, no support for static.
        public boolean updatesStaticRow()
        {
            return false;
        }

        public void addConditions(Clustering clustering, CQL3CasRequest request, QueryOptions options) throws InvalidRequestException
        {
            commitConditions.addConditionsTo(request, clustering, options);
        }

        public NavigableSet<Clustering> createClustering(QueryOptions options)
        throws InvalidRequestException
        {
            return restrictionsForCondition.getClusteringColumns(options);
        }
    }



    private static final ColumnIdentifier TX_RESULT_COLUMN = new ColumnIdentifier("[committed]", false);

    private static final ColumnIdentifier TX_ID_RESULT_COLUMN = new ColumnIdentifier("[tx_id]", false);

    private static final Logger logger = LoggerFactory.getLogger(CommitTransactionStatement.class);

    public CommitTransactionStatement(int boundTermsSize, Term transactionStateAsJson, Optional<CommitConditions> commitConditions)
    {
        this.boundTermsSize = boundTermsSize;
        this.transactionStateAsJson = transactionStateAsJson;
        this.commitConditions = commitConditions;
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
        return executeCommitTransactionStatement(options, state, state.getClientState());
    }

    private static final ColumnIdentifier CAS_RESULT_COLUMN = new ColumnIdentifier("[applied]", false);

    public static ResultSet compareResultSet(TransactionCommitResult transactionCommitResult, CommitConditions commitConditions) {
        boolean success = transactionCommitResult.isConditionFine();

        ColumnSpecification spec = new ColumnSpecification(commitConditions.metadata.ksName, commitConditions.metadata.cfName, CAS_RESULT_COLUMN, BooleanType.instance);
        ResultSet.ResultMetadata metadata = new ResultSet.ResultMetadata(Collections.singletonList(spec));
        List<List<ByteBuffer>> rows = Collections.singletonList(Collections.singletonList(BooleanType.instance.decompose(success)));

        return new ResultSet(metadata, rows);
    }

    private ResultMessage executeCommitTransactionStatement(QueryOptions options, QueryState state, ClientState clientState)
    {
        TransactionState transactionState = MppStatementUtils.getTransactionState(options, transactionStateAsJson);

        Optional<CQL3CasRequest> casRequestOptional = commitConditions.map(x -> x.makeCasRequest(state, options));
        logger.info("CommitTransactionStatement transactionState is " + transactionState);

        try {
            TransactionCommitResult transactionCommitResult =
                MppServicesLocator.getInstance().commitTransaction(transactionState, options.getConsistency(),
                                                                   options, clientState, casRequestOptional);
            // TODO [MPT] Use transactionCommitResult.result to return a row which does not satify condition
            if(transactionCommitResult.getRowIterator() != null) {
                // not satisfied condition, thus not committed.
                CommitConditions conditionsOfCommit = commitConditions.get();
                CQL3CasRequest cql3CasRequest = casRequestOptional.get();
                ResultSet resultSet = ModificationStatement.buildCasResultSet(cql3CasRequest.cfm.ksName,
                                                                              cql3CasRequest.cfm.cfName,
                                                                              transactionCommitResult.getRowIterator(),
                                                                              conditionsOfCommit.getCommitConditions().getColumns(),
                                                                              false,
                                                                              options);
                return MppServiceUtils.transformResultSetToResultMessage(resultSet);
            }
            else if(casRequestOptional.isPresent()) {
                ResultSet compareResultSet = compareResultSet(transactionCommitResult, commitConditions.get());
                boolean committed = transactionCommitResult.isCommitted();
                ResultSet resultSet = buildTxResultSet(committed, transactionState.id());

                return MppServiceUtils.transformResultSetToResultMessage(ModificationStatement.merge(resultSet, compareResultSet));
            }
            else {
                // transaction without condition
                boolean committed = transactionCommitResult.isCommitted();
                ResultSet resultSet = buildTxResultSet(committed, transactionState.id());

                return MppServiceUtils.transformResultSetToResultMessage(resultSet);
            }
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
        return executeCommitTransactionStatement(options, state, state.getClientState());
    }

    public Iterable<Function> getFunctions()
    {
        // TODO [MPP] maybe do something about it
        return Collections.emptyList();
    }

    public static class Parsed extends ParsedStatementWithTransaction
    {

        final Term.Raw transactionStateJson;
        private final CFName nameForCondition;
        private final WhereClause whereForCondition;
        private final boolean ifNotExists;
        private final boolean ifExists;
        private final List<Pair<ColumnIdentifier.Raw, ColumnCondition.Raw>> conditions;

        public Parsed(Term.Raw transactionStateJson, CFName nameForCondition, WhereClause whereForCondition, boolean ifNotExists, boolean ifExists, List<Pair<ColumnIdentifier.Raw, ColumnCondition.Raw>> conditions)
        {
            this.transactionStateJson = transactionStateJson;
            this.nameForCondition = nameForCondition;
            this.whereForCondition = whereForCondition;
            this.ifNotExists = ifNotExists;
            this.ifExists = ifExists;
            this.conditions = conditions;
        }

        //        public Parsed(Term.Raw transactionStateJson)
//        {
//            this.transactionStateJson = transactionStateJson;
//        }

        private Conditions prepareConditions(CFMetaData metadata, VariableSpecifications boundNames)
        {
            // To have both 'IF EXISTS'/'IF NOT EXISTS' and some other conditions doesn't make sense.
            // So far this is enforced by the parser, but let's assert it for sanity if ever the parse changes.
            if (ifExists)
            {
                assert conditions.isEmpty();
                assert !ifNotExists;
                return Conditions.IF_EXISTS_CONDITION;
            }

            if (ifNotExists)
            {
                assert conditions.isEmpty();
                assert !ifExists;
                return Conditions.IF_NOT_EXISTS_CONDITION;
            }

            if (conditions.isEmpty())
                return Conditions.EMPTY_CONDITION;

            return prepareColumnConditions(metadata, boundNames);
        }

        private ColumnConditions prepareColumnConditions(CFMetaData metadata, VariableSpecifications boundNames)
        {
//            checkNull(attrs.timestamp, "Cannot provide custom timestamp for conditional updates");

            ColumnConditions.Builder builder = ColumnConditions.newBuilder();

            for (Pair<ColumnIdentifier.Raw, ColumnCondition.Raw> entry : conditions)
            {
                ColumnIdentifier id = entry.left.prepare(metadata);
                ColumnDefinition def = metadata.getColumnDefinition(id);
                checkNotNull(metadata.getColumnDefinition(id), "Unknown identifier %s in IF conditions", id);

                ColumnCondition condition = entry.right.prepare(nameForCondition.getKeyspace(), def);
                condition.collectMarkerSpecification(boundNames);

                checkFalse(def.isPrimaryKeyColumn(), "PRIMARY KEY column '%s' cannot have IF conditions", id);
                builder.add(condition);
            }
            return builder.build();
        }

        protected static StatementRestrictions newRestrictions(CFMetaData cfm,
                                                               VariableSpecifications boundNames,
                                                               Operations operations,
                                                               WhereClause where,
                                                               Conditions conditions)
        {
            if (where.containsCustomExpressions())
                throw new InvalidRequestException(CUSTOM_EXPRESSIONS_NOT_ALLOWED);

            boolean applyOnlyToStaticColumns = false; // appliesOnlyToStaticColumns(operations, conditions);
            return new StatementRestrictions(StatementType.SELECT, cfm, where, boundNames, applyOnlyToStaticColumns, false, false, false);
        }



        public Prepared prepare() throws RequestValidationException
        {
            Term transactionStateAsJson = this.transactionStateJson != null ? prepareTransactionStateAsJson(this.transactionStateJson) : null;

            CommitTransactionStatement stmt = null;
            if(nameForCondition != null) {
                Operations operations = new Operations(StatementType.SELECT);

                CFMetaData metadata = ThriftValidation.validateColumnFamily(nameForCondition.getKeyspace(), nameForCondition.getColumnFamily());
                VariableSpecifications boundNames = getBoundVariables();
                Conditions commitConditions = prepareConditions(metadata, boundNames);

                assert !commitConditions.isEmpty() || commitConditions.isIfExists() || commitConditions.isIfNotExists();
                StatementRestrictions restrictions = newRestrictions(metadata,
                                                                     boundNames,
                                                                     operations,
                                                                     whereForCondition,
                                                                     commitConditions);

                CommitConditions conditionsForCommit = new CommitConditions(metadata, restrictions, commitConditions, operations);
                stmt = new CommitTransactionStatement(getBoundVariables().size(), transactionStateAsJson, Optional.of(conditionsForCommit));
            }
            else {
                stmt = new CommitTransactionStatement(getBoundVariables().size(), transactionStateAsJson, Optional.empty());
            }
            return new Prepared(stmt, getBoundVariables(), null);
        }
    }
}
