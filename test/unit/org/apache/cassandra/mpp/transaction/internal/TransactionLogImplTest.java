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

import java.util.Optional;

import org.junit.Test;

import com.datastax.driver.core.utils.UUIDs;
import junit.framework.Assert;
import org.apache.cassandra.mpp.MppServicesLocator;
import org.apache.cassandra.mpp.transaction.MppCQLTester;
import org.apache.cassandra.mpp.transaction.MppTransactionLog;
import org.apache.cassandra.mpp.transaction.TransactionId;
import org.apache.cassandra.mpp.transaction.TransactionTimeUUID;
import org.joda.time.DateTime;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 03/05/16
 */
public class TransactionLogImplTest extends MppCQLTester
{
    @Test
    public void shouldInsertAndFindTransactionAsCommitted() {
        MppTransactionLog transactionLog = MppServicesLocator.getTransactionLog();

        DateTime dateTime = new DateTime();
        TransactionId id1 = new TransactionTimeUUID(UUIDs.startOf(dateTime.getMillis()));
        TransactionId id2 = new TransactionTimeUUID(UUIDs.startOf(dateTime.plusMillis(10).getMillis()));

        transactionLog.appendCommitted(id1);

        Assert.assertTrue(transactionLog.existsInLog(id1));
        Assert.assertFalse(transactionLog.existsInLog(id2));

        Assert.assertTrue(transactionLog.txWasCommitted(id1));

        Optional<MppTransactionLog.TxLog> txLog = transactionLog.checkTransactionInLog(id1);
        Assert.assertTrue(txLog.isPresent());
        Assert.assertEquals(MppTransactionLog.TxLog.COMMITTED, txLog.get());

        transactionLog.appendRolledBack(id1); // cannot append twice
        Assert.assertTrue(transactionLog.txWasCommitted(id1));
    }

    @Test
    public void shouldKnowWhenTransactionIsUnknownToLog() {
        MppTransactionLog transactionLog = MppServicesLocator.getTransactionLog();

        DateTime dateTime = new DateTime();
        TransactionId id1 = new TransactionTimeUUID(UUIDs.startOf(dateTime.getMillis()));

        Assert.assertFalse(transactionLog.existsInLog(id1));
    }
}
