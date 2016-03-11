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
import org.apache.cassandra.mpp.transaction.MppServiceUtils;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;
import org.apache.cassandra.mpp.transaction.client.TransactionState;

import static org.junit.Assert.assertEquals;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 20/02/16
 */
public class RepairPrivateDataScenarioTest extends BaseClusterTest
{

    @Test
    public void shouldRepairTransactionDataOnRequest() throws Throwable {

        Session sessionN1 = getSessionN1();

        TransactionState tx = startTransaction(sessionN1);

        MppTestSchemaHelpers.Item tShirt = MppTestSchemaHelpers.Item.newItem("T-Shirt", "10$");
        MppTestSchemaHelpers.Item phone = MppTestSchemaHelpers.Item.newItem("Phone", "450$");

        String userName = "john doe";
        MppTestSchemaHelpers.UserItems userItem = MppTestSchemaHelpers.UserItems.newUserItem(userName, tShirt);
        MppTestSchemaHelpers.UserItems userItem2 = MppTestSchemaHelpers.UserItems.newUserItem(userName, phone);

        tx = tx.merge(MppTestSchemaHelpers.Item.persistItem(sessionN1, tShirt, tx));
        TransactionState afterPersistedPhone = MppTestSchemaHelpers.Item.persistItem(sessionN1, phone, tx);
        assertEquals(1, afterPersistedPhone.getTransactionItems().size());
        final TransactionItem phoneTransactionItem = afterPersistedPhone.getTransactionItems().iterator().next();
        tx = tx.merge(afterPersistedPhone);
        tx = tx.merge(MppTestSchemaHelpers.UserItems.persistUserItem(sessionN1, userItem, tx));
        tx = tx.merge(MppTestSchemaHelpers.UserItems.persistUserItem(sessionN1, userItem2, tx));

        Assert.assertEquals("Should have 3 transaction items because user_items has partition key with item_id only", 3, tx.getTransactionItems().size());
        System.out.println(tx.getTransactionItems());
//        TransactionItem transactionItemForUserItems = tx.getTransactionItems().stream().filter(ti -> ti.getCfName().equals("user_items")).findFirst().get();

        ResultSet resultSet = sessionN1.execute("READ TRANSACTIONAL LOCALLY TRANSACTION " +
                                              tx.getTransactionId()
                                              + " FROM mpptest.items");
        System.out.println("ResultSet after inserts " + resultSet);
        assertEquals(2, resultSet.all().size());

        getNodeProbe1().getMppProxy().deleteSingleItem(MppServiceUtils.getTransactionStateAsJson(tx), MppServiceUtils.getTransactionItemAsJson(phoneTransactionItem));

        ResultSet resultSetAfterDeleted = sessionN1.execute("READ TRANSACTIONAL LOCALLY TRANSACTION " +
                                                tx.getTransactionId()
                                                + " FROM mpptest.items");
        assertEquals(1, resultSetAfterDeleted.all().size());
        System.out.println("ResultSet after deleted single item " + resultSet);

        System.out.println("Making consistent data ");
        getNodeProbe1().getMppProxy().makeTransactionDataConsistent(MppServiceUtils.getTransactionStateAsJson(tx));
        System.out.println("Made consistent data ");

        ResultSet resultSetAfterMadeConsistent = sessionN1.execute("READ TRANSACTIONAL LOCALLY TRANSACTION " +
                                                            tx.getTransactionId()
                                                            + " FROM mpptest.items");

        System.out.println("ResultSet after made consistent " + resultSet);

        assertEquals(2, resultSetAfterMadeConsistent.all().size());
    }
}
