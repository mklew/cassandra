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

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import junit.framework.Assert;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.tools.NodeProbe;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 04/04/16
 */
public class MultiPartitionPaxosTest extends BaseClusterTest
{

    @Before
    public void clearListsOfCommittedAndRolledBack() {
        getNodeProbesStream().forEach(nodeProbe -> nodeProbe.getMppProxy().clearLists());
    }

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

        commitTransaction(sessionN1, transactionState);
        ResultSet execute = sessionN1.execute("SELECT * FROM mpptest.items WHERE item_id = ?", itemForPartition1.itemId);
        Assert.assertEquals("descriptions of item for partition 1 should match", itemForPartition1Desc, execute.one().getString("item_description"));

        ResultSet execute2 = sessionN1.execute("SELECT * FROM mpptest.items WHERE item_id = ?", itemForPartition2.itemId);
        Assert.assertEquals("descriptions of item for partition 2 should match", itemForPartition2Desc, execute2.one().getString("item_description"));
    }

    /**
     * Scenario:
     *
     * Two transactions modify:
     *  - same partition
     *  - and other independent partitions
     *
     *  After execution, only one should succeed (in most cases, because it depends on timing).
     *
     *  See which succeeded based on results of modifications of common partition.
     *
     *  Check if independent partitions have expected that.
     *  Check if independent partitions from rolledback transaction are empty.
     *
     * @throws Throwable
     */
    @Test
    public void shouldRollbackConcurrentTransactionWithManyPartitions() throws Throwable {
        MppTestSchemaHelpers.Item commonItem = createSingleItem();
        Session sessionN1 = getSessionN1();
        Session sessionN2 = getSessionN2();

        // 1. Given Two transactions
        TransactionState tx1 = beginTransaction(sessionN1);
        TransactionState tx2 = beginTransaction(sessionN2);

        UUID tx1Id = tx1.getTransactionId();
        UUID tx2Id = tx2.getTransactionId();
        // Wait for them in multi partition paxos, allow them to proceed when both of them are registered in index.
//        getNodeProbesStream().forEach(nodeProbe -> {
//            nodeProbe.getMppProxy().storageProxyExtAddToWaitUntilAfterPrePrepared(tx1Id.toString(), tx2Id.toString());
//        });

        // 2. Transactions modify same piece of data.
        MppTestSchemaHelpers.Item itemForTx1 = commonItem.copyWithDescription("tx 1 description");

        MppTestSchemaHelpers.Item itemForTx2 = commonItem.copyWithPrice("tx 2 price");

        tx1 = tx1.merge(MppTestSchemaHelpers.Item.persistItem(sessionN1, itemForTx1, tx1));
        tx2 = tx2.merge(MppTestSchemaHelpers.Item.persistItem(sessionN2, itemForTx2, tx2));

        // Independent data for Tx1
        final MppTestSchemaHelpers.Item tx1Item1 = MppTestSchemaHelpers.Item.newItemWithDescription("tx1 item 1");
        final MppTestSchemaHelpers.Item tx1Item2 = MppTestSchemaHelpers.Item.newItemWithDescription("tx1 item 2");
        final MppTestSchemaHelpers.Item tx1Item3 = MppTestSchemaHelpers.Item.newItemWithDescription("tx1 item 3");

        // persist for Tx1
        tx1 = tx1.merge(MppTestSchemaHelpers.Item.persistItem(sessionN1, tx1Item1, tx1));
        tx1 = tx1.merge(MppTestSchemaHelpers.Item.persistItem(sessionN1, tx1Item2, tx1));
        tx1 = tx1.merge(MppTestSchemaHelpers.Item.persistItem(sessionN1, tx1Item3, tx1));

        // Independent data for Tx2
        final MppTestSchemaHelpers.Item tx2Item1 = MppTestSchemaHelpers.Item.newItemWithDescription("tx2 item 1");
        final MppTestSchemaHelpers.Item tx2Item2 = MppTestSchemaHelpers.Item.newItemWithDescription("tx2 item 2");

        // persist for Tx2
        tx2 = tx2.merge(MppTestSchemaHelpers.Item.persistItem(sessionN2, tx2Item1, tx2));
        tx2 = tx2.merge(MppTestSchemaHelpers.Item.persistItem(sessionN2, tx2Item2, tx2));

        commitTransactionAsync(sessionN1, tx1);
        commitTransactionAsync(sessionN2, tx2);

        Thread.sleep(10000);
        MppTestSchemaHelpers.Item foundItem = MppTestSchemaHelpers.Item.findItemById(commonItem.itemId, sessionN1);

        if(foundItem.description == null && foundItem.price == null) {
            Assert.fail("Nothing worked, something went bad. Check logs");
        }

        if(foundItem.description != null && foundItem.price != null) {
            Assert.fail("Timing was bad and both succeeded. Run test again.");
        }

        if(foundItem.description != null) {
            // First transaction succeeded. Check data for presence.
            System.out.println("Tx1 succeeded");
            Assert.assertTrue("items of Tx1 should exist", itemsOfTx1Exist(sessionN1, tx1Item1, tx1Item2, tx1Item3));
            Assert.assertFalse("items of Tx2 should NOT exist", itemsOfTx2Exist(sessionN2, tx2Item1, tx2Item2));
        }
        else {
            // Second transaction succeeded. Check data for presence.
            System.out.println("Tx2 succeeded");
            Assert.assertFalse("items of Tx1 should NOT exist", itemsOfTx1Exist(sessionN1, tx1Item1, tx1Item2, tx1Item3));
            Assert.assertTrue("items of Tx2 should exist", itemsOfTx2Exist(sessionN2, tx2Item1, tx2Item2));
        }

        System.out.println("Test is done");
    }

    private void displaySummaryOfCommits()
    {
        System.out.println("Printing committed and rolled back transactions at each node");
        getNodeProbesNamedStream().forEach(namedProbe -> {
            String info = "For node " + namedProbe.name;
            NodeProbe nodeProbe = namedProbe.nodeProbe;
            String [] committed1 = nodeProbe.getMppProxy().listOfCommittedTransactions();
            List<String> committed = getListOf(committed1);
            info = info + "\n" + "Committed transactions: " + committed;
            List<String> rolledBack = getListOf(nodeProbe.getMppProxy().listOfRolledBackTransactions());
            info = info + "\n" + "Rolled back transactions: " + rolledBack;
            List<String> historyOfCommitRollback = getListOf(nodeProbe.getMppProxy().listOfCommittedAndRolledBack());
            info = info + "\n" + "Order: " + historyOfCommitRollback;

            System.out.println(info);
        });
    }

    private static boolean itemsOfTx1Exist(Session sessionN1, MppTestSchemaHelpers.Item tx1Item1, MppTestSchemaHelpers.Item tx1Item2, MppTestSchemaHelpers.Item tx1Item3)
    {
        Optional<MppTestSchemaHelpers.Item> i1pt = MppTestSchemaHelpers.Item.findItemByIdOptional(tx1Item1.itemId, sessionN1);
        Optional<MppTestSchemaHelpers.Item> i2pt = MppTestSchemaHelpers.Item.findItemByIdOptional(tx1Item2.itemId, sessionN1);
        Optional<MppTestSchemaHelpers.Item> i3pt = MppTestSchemaHelpers.Item.findItemByIdOptional(tx1Item3.itemId, sessionN1);
        i1pt.ifPresent(item -> Assert.assertEquals("tx1 item 1", item.description));
        i2pt.ifPresent(item -> Assert.assertEquals("tx1 item 2", item.description));
        i3pt.ifPresent(item -> Assert.assertEquals("tx1 item 3", item.description));

        return i1pt.isPresent() && i2pt.isPresent() && i3pt.isPresent();
    }

    private static boolean itemsOfTx2Exist(Session sessionN2, MppTestSchemaHelpers.Item tx2Item1, MppTestSchemaHelpers.Item tx2Item2)
    {
        Optional<MppTestSchemaHelpers.Item> i1pt = MppTestSchemaHelpers.Item.findItemByIdOptional(tx2Item1.itemId, sessionN2);
        Optional<MppTestSchemaHelpers.Item> i2pt = MppTestSchemaHelpers.Item.findItemByIdOptional(tx2Item2.itemId, sessionN2);
        i1pt.ifPresent(item -> Assert.assertEquals("tx2 item 1", item.description));
        i2pt.ifPresent(item -> Assert.assertEquals("tx2 item 2", item.description));

        return i1pt.isPresent() && i2pt.isPresent();
    }


    @Test
    public void shouldRollbackConcurrentTransaction() throws InterruptedException
    {
        // Item to create
        MppTestSchemaHelpers.Item item = createSingleItem();

        Session sessionN1 = getSessionN1();
        Session sessionN2 = getSessionN2();

        // 1. Given Two transactions
        TransactionState tx1 = beginTransaction(sessionN1);
        TransactionState tx2 = beginTransaction(sessionN2);

        UUID tx1Id = tx1.getTransactionId();
        UUID tx2Id = tx2.getTransactionId();
        // Wait for them in multi partition paxos, allow them to proceed when both of them are registered in index.
        getNodeProbesStream().forEach(nodeProbe -> {
            nodeProbe.getMppProxy().storageProxyExtAddToWaitUntilAfterPrePrepared(tx1Id.toString(), tx2Id.toString());
        });

        // 2. Transactions modify same piece of data.
        MppTestSchemaHelpers.Item itemForTx1 = item.copyWithDescription("tx 1 description");

        MppTestSchemaHelpers.Item itemForTx2 = item.copyWithPrice("tx 2 price");

        tx1 = tx1.merge(MppTestSchemaHelpers.Item.persistItem(sessionN1, itemForTx1, tx1));
        tx2 = tx2.merge(MppTestSchemaHelpers.Item.persistItem(sessionN2, itemForTx2, tx2));

        ExecutorService executor = Executors.newFixedThreadPool(1);

        ResultSetFuture tx1ResultSetF = commitTransactionAsync(sessionN1, tx1);
        tx1ResultSetF.addListener(() -> {
            System.out.println("Tx1 future has finished. Tx1 ID is " + tx1Id);
            ResultSet rows = null;
            try
            {
                rows = tx1ResultSetF.get();
                Row one = rows.one();
                UUID txId = one.getUUID("[tx_id]");
                boolean committed = one.getBool("[committed]");
                if (committed)
                    System.out.println("Transaction with ID " + txId + " was committed");
                else
                    System.out.println("Transaction with ID " + txId + " was rolled back");
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
            catch (ExecutionException e)
            {
                e.printStackTrace();
            }
        }, executor);
        ResultSetFuture tx2ResultSetF = commitTransactionAsync(sessionN2, tx2);
        tx2ResultSetF.addListener(() -> {
            System.out.println("Tx2 future has finished. Tx2 ID is " + tx2Id);
            ResultSet rows = null;
            try
            {
                rows = tx2ResultSetF.get();
                Row one = rows.one();
                UUID txId = one.getUUID("[tx_id]");
                boolean committed = one.getBool("[committed]");
                if (committed)
                    System.out.println("Transaction with ID " + txId + " was committed");
                else
                    System.out.println("Transaction with ID " + txId + " was rolled back");
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
            catch (ExecutionException e)
            {
                e.printStackTrace();
            }
        }, executor);

        Thread.sleep(10000);
        MppTestSchemaHelpers.Item foundItem = MppTestSchemaHelpers.Item.findItemById(item.itemId, sessionN1);

        System.out.println("Item is: " + foundItem);

        if(foundItem.description == null && foundItem.price == null) {
            Assert.fail("Both description and price were null");
        }

        if(foundItem.description != null && foundItem.price != null) {
            Assert.fail("Both should not be defined");
        }

        // 3. Try to commit at same time.
            // TODO [MPP] Need to wait on both of them just after PRE PREPARE so they are both registered in index, but do not proceed
        // 4. After, only one should get saved, other should not be able to proceed - it will fail on next phase
        displaySummaryOfCommits();

    }

    private MppTestSchemaHelpers.Item createSingleItem()
    {
        Session sessionN1 = getSessionN1();
        final MppTestSchemaHelpers.Item item = MppTestSchemaHelpers.Item.newItem();
        MppTestSchemaHelpers.Item createdItem = MppTestSchemaHelpers.Item.persistItemWithoutTx(sessionN1, item);
        return createdItem;
    }
}
