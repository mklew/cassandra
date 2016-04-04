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

package mpp;

import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import junit.framework.Assert;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.mpp.transaction.client.dto.TransactionStateDto;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 04/04/16
 */
public class MultiPartitionPaxosTest extends BaseClusterTest
{

    @Test
    public void shouldCommitTwoPartitionsAtSameTime() {
        Session sessionN1 = getSessionN1();

        TransactionState transactionState = beginTransaction(sessionN1);

        String itemForPartition1Desc = "Some description";
        final MppTestSchemaHelpers.Item itemForPartition1 = MppTestSchemaHelpers.Item.newItem(itemForPartition1Desc, "10 usd");
        String itemForPartition2Desc = "priceless";
        final MppTestSchemaHelpers.Item itemForPartition2 = MppTestSchemaHelpers.Item.newItemWithDescription(itemForPartition2Desc);

        transactionState = transactionState.merge(MppTestSchemaHelpers.Item.persistItem(sessionN1, itemForPartition1, transactionState));
        transactionState = transactionState.merge(MppTestSchemaHelpers.Item.persistItem(sessionN1, itemForPartition2, transactionState));

        final TransactionStateDto transactionStateDto = TransactionStateDto.fromTransactionState(transactionState);
        final String txStateJson = MppTest.getJson(transactionStateDto);

        // TODO [MPP] I get operation time out when trying to use prepared statement. None of hosts can handle that prepared statement.

//        PreparedStatement preparedCommitTransactionStmt = sessionN1.prepare("COMMIT TRANSACTION AS JSON ?");
//        BoundStatement boundStatement = preparedCommitTransactionStmt.bind("'" + txStateJson + "'");
//        sessionN1.execute(boundStatement);

        // Json had to be wrapped in single quotes
        sessionN1.execute("COMMIT TRANSACTION AS JSON '" + txStateJson + "'");
        ResultSet execute = sessionN1.execute("SELECT * FROM mpptest.items WHERE item_id = ?", itemForPartition1.itemId);
        Assert.assertEquals("descriptions of item for partition 1 should match", itemForPartition1Desc, execute.one().getString("item_description"));

        ResultSet execute2 = sessionN1.execute("SELECT * FROM mpptest.items WHERE item_id = ?", itemForPartition2.itemId);
        Assert.assertEquals("descriptions of item for partition 2 should match", itemForPartition2Desc, execute2.one().getString("item_description"));
    }

}
