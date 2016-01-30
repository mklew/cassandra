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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.base.Preconditions;

import com.datastax.driver.core.Row;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.Json;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.TransactionalMutation;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.mpp.transaction.client.dto.TransactionStateDto;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.Pair;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 23/01/16
 */
public class MppServiceUtils
{

    public static final String KS_NAME = "mpp_extension";
    public static final String TRANSACTION_STATE_CF_NAME = "transaction_states";

    public static final String TRANSACTION_ID_NAME_COL = "transaction_id";
    public static final String KS_NAME_COL = "ks_name";
    public static final String CF_NAME_COL = "cf_name";
    public static final String TOKEN_NAME_COL = "token";

    private static List<ColumnSpecification> transactionStateMetaData;

//    private static final CFMetaData TransactionStates =
//    compile(TRANSATION_STATES,
//            "transaction states",
//            "CREATE TABLE %s ("
//            + "transaction_id timeuuid,"
//            + "ks_name text,"
//            + "cf_name text,"
//            + "token long,"
//            + "PRIMARY KEY(transaction_id, (ks_name, cf_name, token)))");

//
//    private static CFMetaData compile(String name, String description, String schema)
//    {
//        return CFMetaData.compile(String.format(schema, name), MPP_KEYSPACE)
//                         .comment(description)
//                         .gcGraceSeconds((int) TimeUnit.DAYS.toSeconds(90));
//    }

//    public CFMetaData getTransactionStatesCfMetaData() {
//        return TransactionStates;
//    }

    static
    {
        transactionStateMetaData = getColumnSpecificationsForTransactionState();
    }

    private static List<ColumnSpecification> getColumnSpecificationsForTransactionState()
    {
        List<ColumnSpecification> columns = new ArrayList<>(4);
        columns.add(new ColumnSpecification(KS_NAME, TRANSACTION_STATE_CF_NAME, new ColumnIdentifier(TRANSACTION_ID_NAME_COL, true), UUIDType.instance));
        columns.add(new ColumnSpecification(KS_NAME, TRANSACTION_STATE_CF_NAME, new ColumnIdentifier(KS_NAME_COL, true), UTF8Type.instance));
        columns.add(new ColumnSpecification(KS_NAME, TRANSACTION_STATE_CF_NAME, new ColumnIdentifier(CF_NAME_COL, true), UTF8Type.instance));
        columns.add(new ColumnSpecification(KS_NAME, TRANSACTION_STATE_CF_NAME, new ColumnIdentifier(TOKEN_NAME_COL, true), LongType.instance));
        return Collections.unmodifiableList(columns);
    }


    private MppServiceUtils()
    {
    }

    static ResultSet newTransactionStateResultSet()
    {
        return new ResultSet(transactionStateMetaData);
    }

    static Function<TransactionState, Function<ResultSet, ResultSet>> addTxItemToResultSet = txState -> resultSet -> {
        assert txState.getTransactionItems().size() == 1;
        final TransactionItem txItem = txState.getTransactionItems().iterator().next();

        resultSet.addColumnValue(transactionIdAsColumn(txState));
        resultSet.addColumnValue(keyspaceNameAsColumn(txItem));
        resultSet.addColumnValue(columnFamilyNameAsColumn(txItem));
        resultSet.addColumnValue(tokenAsColumn(txItem));
        return resultSet;
    };

    private static ByteBuffer tokenAsColumn(TransactionItem txItem)
    {
        final Token token = txItem.getToken();
        Preconditions.checkArgument(token instanceof Murmur3Partitioner.LongToken, "Expected token: " + token + " to be instance of Murmur3Partitioner.LongToken");
        Murmur3Partitioner.LongToken longToken = (Murmur3Partitioner.LongToken) token;
        return LongType.instance.decompose((Long) longToken.getTokenValue());
    }

    private static ByteBuffer columnFamilyNameAsColumn(TransactionItem txItem)
    {
        return UTF8Type.instance.decompose(txItem.getCfName());
    }

    private static ByteBuffer keyspaceNameAsColumn(TransactionItem txItem)
    {
        return UTF8Type.instance.decompose(txItem.getKsName());
    }

    public static ResultMessage transformResultSetToResultMessage(ResultSet resultSet)
    {
        if (resultSet.isEmpty()) return new ResultMessage.Void();
        else return new ResultMessage.Rows(resultSet);
    }

    private static ColumnSpecification getJsonColumn() {
        return new ColumnSpecification(KS_NAME, TRANSACTION_STATE_CF_NAME, Json.JSON_COLUMN_ID, UTF8Type.instance);
    }

    public static ResultSet mapTransactionStateToJson(TransactionState transactionState)
    {
        Preconditions.checkNotNull(transactionState);
        final ColumnSpecification jsonColumn = getJsonColumn();
        final List<ColumnSpecification> columnSpecifications = Arrays.asList(jsonColumn);
        final ResultSet resultSet = new ResultSet(columnSpecifications);

        final String json;
        try
        {
            json = Json.JSON_OBJECT_MAPPER.writeValueAsString(TransactionStateDto.fromTransactionState(transactionState));
        }
        catch (IOException e)
        {
            throw new RuntimeException("Cannot write TransactionState as json", e);
        }

        resultSet.addColumnValue(UTF8Type.instance.decompose(json));
        return resultSet;
    }

    public static ResultSet executeFnOverTransactionalMutations(Collection<? extends IMutation> mutations, Function<TransactionalMutation, TransactionState> getTransactionItemFn)
    {
        final ResultSet resultSet = newTransactionStateResultSet();
        mutations.stream()
                 .filter(m -> m instanceof TransactionalMutation)
                 .map(m -> (TransactionalMutation) m)
                 .map(getTransactionItemFn)
                 .map(addTxItemToResultSet::apply)
                 .forEach(f -> f.apply(resultSet));

        return resultSet;
    }

    /**
     * @param mutations
     * @return pair with result message and numer of executed mutations
     */
    public static Pair<ResultMessage, Integer> executeTransactionalMutations(Collection<? extends IMutation> mutations)
    {
        final ResultSet resultSet = executeFnOverTransactionalMutations(mutations, TransactionalMutation::applyAndGetAsTransactionState);
        final ResultMessage resultMessage = transformResultSetToResultMessage(resultSet);
        return Pair.create(resultMessage, resultSet.size());
    }

    /**
     * Passed in mutations have already been successfully executed.
     *
     * If there are no transactional mutations then return null, else result set
     *
     * @param mutations
     * @return
     */
    public static ResultMessage successfulMutationsToResultMessage(Collection<? extends IMutation> mutations)
    {
        if (mutations.stream().anyMatch(m -> m instanceof TransactionalMutation))
        {
            final ResultSet resultSet = executeFnOverTransactionalMutations(mutations, TransactionalMutation::toTransactionState);
            final ResultMessage resultMessage = transformResultSetToResultMessage(resultSet);
            return resultMessage;
        }
        else
        {
            return null;
        }
    }

    public static Stream<UntypedResultSet.Row> streamResultSet(UntypedResultSet resultSet)
    {
        final Iterator<UntypedResultSet.Row> iterator = resultSet.iterator();
        Iterable<UntypedResultSet.Row> iterable = () -> iterator;
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    public static ResultSet mapTransactionStateToResultSet(TransactionState transactionState, boolean isJson)
    {
        if (isJson)
            return mapTransactionStateToJson(transactionState);
        else
            return mapTransactionStateToRows(transactionState);
    }

    public static ResultSet mapTransactionStateToRows(TransactionState transactionState)
    {
        final ResultSet resultSet = new ResultSet(transactionStateMetaData);

        addTransactionStateToResultSet(transactionState, resultSet);

        return resultSet;
    }

    private static void addTransactionStateToResultSet(TransactionState transactionState, ResultSet resultSet)
    {
        if(transactionState.getTransactionItems().isEmpty()) {
            // Just add single column value
            resultSet.addColumnValue(transactionIdAsColumn(transactionState));
            resultSet.addColumnValue(null);
            resultSet.addColumnValue(null);
            resultSet.addColumnValue(null);
        }
        else {
            transactionState.getTransactionItems().stream().forEach(txItem -> {
                resultSet.addRow(Arrays.asList(transactionIdAsColumn(transactionState),
                                               keyspaceNameAsColumn(txItem),
                                               columnFamilyNameAsColumn(txItem),
                                               tokenAsColumn(txItem)));
            });
        }
    }

    private static ByteBuffer transactionIdAsColumn(TransactionState transactionState)
    {
        return UUIDType.instance.decompose(transactionState.getTransactionId());
    }

    public static Stream<Row> streamResultSet(com.datastax.driver.core.ResultSet resultSet)
    {
        return StreamSupport.stream(resultSet.spliterator(), false);
    }

    // TODO [MPP] I guess it is not required so far
//    public static KeyspaceMetadata metadata()
//    {
//        return KeyspaceMetadata.create(MPP_KEYSPACE, KeyspaceParams.simple(1), Tables.of(TransactionStates));
//    }
}
