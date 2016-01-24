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

package org.apache.cassandra.mpp.transaction.internal;

import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.mpp.transaction.MppCQLTester;
import org.apache.cassandra.mpp.transaction.client.TransactionState;

import static org.apache.cassandra.mpp.transaction.MppTestingUtilities.START_TRANSACTION;
import static org.apache.cassandra.mpp.transaction.MppTestingUtilities.mapResultToTransactionState;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 24/01/16
 */
public class StartTransactionStatementTest extends MppCQLTester
{

    @Test
    public void shouldBeginTransactionAndReturnItsInitialTransactionState() throws Throwable
    {
        final UntypedResultSet resultSet = execute(START_TRANSACTION);
        Assert.assertNotNull("Start transaction should successfully return results", resultSet);

        TransactionState txState = mapResultToTransactionState(resultSet);

        final long txTimestamp = UUIDs.unixTimestamp(txState.getTransactionId());
        Assert.assertTrue("Timestamp means that timeuuid was received", txTimestamp > 0);
    }


}
