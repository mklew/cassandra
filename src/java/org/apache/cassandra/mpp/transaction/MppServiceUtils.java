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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.base.Preconditions;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.TransactionalMutation;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.Pair;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 23/01/16
 */
public class MppServiceUtils
{


    public static final String KS_NAME_COL = "ks_name";
    public static final String CF_NAME_COL = "cf_name";
    public static final String TOKEN_NAME_COL = "token";
    private static List<ColumnSpecification> metadata;

    static
    {
        metadata = getColumnSpecifications();
    }

    public static final String KS_NAME = "mpp_extension";
    public static final String CF_NAME = "transaction_items";

    private MppServiceUtils()
    {
    }

    static ResultSet newTransactionItemsResultSet()
    {
        return new ResultSet(metadata);
    }

    static Function<TransactionItem, Function<ResultSet, ResultSet>> addTxItemToResultSet = txItem -> resultSet -> {
        addTransactionItemToResultSet(txItem, resultSet);
        return resultSet;
    };

    private static void addTransactionItemToResultSet(TransactionItem txItem, ResultSet result)
    {
        result.addColumnValue(UTF8Type.instance.decompose(txItem.getKsName()));
        result.addColumnValue(UTF8Type.instance.decompose(txItem.getCfName()));
        final Token token = txItem.getToken();
        Preconditions.checkArgument(token instanceof Murmur3Partitioner.LongToken, "Expected token: " + token + " to be instance of Murmur3Partitioner.LongToken");
        Murmur3Partitioner.LongToken longToken = (Murmur3Partitioner.LongToken) token;
        result.addColumnValue(LongType.instance.decompose((Long) longToken.getTokenValue()));
    }

    private static List<ColumnSpecification> getColumnSpecifications()
    {
        List<ColumnSpecification> columns = new ArrayList<>(3);
        columns.add(new ColumnSpecification(KS_NAME, CF_NAME, new ColumnIdentifier(KS_NAME_COL, true), UTF8Type.instance));
        columns.add(new ColumnSpecification(KS_NAME, CF_NAME, new ColumnIdentifier(CF_NAME_COL, true), UTF8Type.instance));
        columns.add(new ColumnSpecification(KS_NAME, CF_NAME, new ColumnIdentifier(TOKEN_NAME_COL, true), LongType.instance));
        return Collections.unmodifiableList(columns);
    }

    public static ResultMessage transformResultSetToResultMessage(ResultSet resultSet)
    {
        if (resultSet.isEmpty()) return new ResultMessage.Void();
        else return new ResultMessage.Rows(resultSet);
    }

    public static ResultSet executeFnOverTransactionalMutations(Collection<? extends IMutation> mutations, Function<TransactionalMutation, TransactionItem> getTransactionItemFn)
    {
        final ResultSet resultSet = newTransactionItemsResultSet();
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
        final ResultSet resultSet = executeFnOverTransactionalMutations(mutations, TransactionalMutation::apply);
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
            final ResultSet resultSet = executeFnOverTransactionalMutations(mutations, TransactionalMutation::toTransactionItem);
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
}
