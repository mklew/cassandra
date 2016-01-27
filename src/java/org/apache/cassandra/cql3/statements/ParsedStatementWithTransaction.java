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

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.mpp.transaction.MppServiceUtils;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 24/01/16
 */
public abstract class ParsedStatementWithTransaction extends ParsedStatement
{

    protected static Term prepareTransactionId(Term.Raw transactionId)
    {
        return transactionId.prepare(MppServiceUtils.KS_NAME, transactionIdReceiver());
    }

    protected static ColumnSpecification transactionIdReceiver()
    {
        return new ColumnSpecification(MppServiceUtils.KS_NAME, MppServiceUtils.TRANSACTION_STATE_CF_NAME, new ColumnIdentifier("[transaction_id]", true), UUIDType.instance);
    }

    protected static Term prepareToken(Term.Raw token)
    {
        return token.prepare(MppServiceUtils.KS_NAME, tokenReceiver());
    }

    protected static Term prepareTransactionStateAsJson(Term.Raw transactionStateAsJson)
    {
        return transactionStateAsJson.prepare(MppServiceUtils.KS_NAME, transactionStateReceiver());
    }

    protected static ColumnSpecification transactionStateReceiver()
    {
        return new ColumnSpecification(MppServiceUtils.KS_NAME, MppServiceUtils.TRANSACTION_STATE_CF_NAME, new ColumnIdentifier("[transaction_state]", true), UTF8Type.instance);
    }


    protected static ColumnSpecification tokenReceiver()
    {
        return new ColumnSpecification(MppServiceUtils.KS_NAME, MppServiceUtils.TRANSACTION_STATE_CF_NAME, new ColumnIdentifier(MppServiceUtils.TOKEN_NAME_COL, true), LongType.instance);
    }
}
