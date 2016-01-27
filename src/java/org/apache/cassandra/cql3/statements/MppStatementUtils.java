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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.apache.cassandra.cql3.Json;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.mpp.transaction.client.TransactionStateUtils;
import org.apache.cassandra.mpp.transaction.client.dto.TransactionStateDto;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 24/01/16
 */
public class MppStatementUtils
{
    public static UUID getTransactionId(QueryOptions options, Term transactionIdTerm)
    {
        ByteBuffer bb = checkNotNull(transactionIdTerm.bindAndGet(options), "Invalid null value of transaction id");
        UUIDType.instance.validate(bb);
        final UUID transactionId = UUIDType.instance.compose(bb);
        return transactionId;
    }

    public static Murmur3Partitioner.LongToken getToken(QueryOptions options, Term preparedToken)
    {
        final ByteBuffer bb = checkNotNull(preparedToken.bindAndGet(options), "Invalud null value of token");
        LongType.instance.validate(bb);
        final Long t = LongType.instance.compose(bb);

        return new Murmur3Partitioner.LongToken(t);
    }

    public static TransactionState getTransactionState(QueryOptions options, Term transactionStateAsJson)
    {
        final ByteBuffer bb = checkNotNull(transactionStateAsJson.bindAndGet(options), "Invalud null value of transaction state as json");
        UTF8Type.instance.validate(bb);
        try
        {
            final TransactionStateDto transactionStateDto = Json.JSON_OBJECT_MAPPER.readValue(ByteBufferUtil.getArray(bb), TransactionStateDto.class);

            return TransactionStateUtils.compose(transactionStateDto);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
