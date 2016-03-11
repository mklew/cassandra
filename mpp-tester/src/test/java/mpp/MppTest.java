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
import java.util.UUID;

import org.junit.Test;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import junit.framework.Assert;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.mpp.transaction.client.dto.TransactionStateDto;

import static mpp.MppTestSchemaHelpers.Item;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 20/01/16
 */
public class MppTest extends BaseClusterTest
{


//    create table mpptest.items (item_id uuid, item_description text, price text, primary key (item_id));


    @Test
    public void testWritingAndReadingQuorumScenario() {
        Session sessionN1 = getSessionN1();

        TransactionState transactionState = beginTransaction(sessionN1);

        final Item itemWithDescriptionAndPrice = Item.newItem("Some description", "10 usd");
        final Item priceless = Item.newItemWithDescription("priceless");
        final Item itemWithJustId = Item.newItem();

        // Do writes to private memtable
        transactionState = transactionState.merge(Item.persistItem(sessionN1, itemWithDescriptionAndPrice, transactionState));
        final TransactionState afterPriceless = Item.persistItem(sessionN1, priceless, transactionState);
        final Murmur3Partitioner.LongToken pricelessToken = afterPriceless.singleToken();
        transactionState = transactionState.merge(afterPriceless);
        transactionState = transactionState.merge(Item.persistItem(sessionN1, itemWithJustId, transactionState));

        final TransactionStateDto transactionStateDto = TransactionStateDto.fromTransactionState(transactionState);


        final String txStateJson = getJson(transactionStateDto);

        final String stmt = "READ TRANSACTIONAL TRANSACTION AS JSON '" + txStateJson + "' FROM " + "mpptest.items TOKEN " + pricelessToken.getTokenValue();
        final SimpleStatement simpleStatement = new SimpleStatement(stmt);
        // TODO [MPP] Modify driver to have consistency levels
        // TODO [MPP] Figure out why it doesn't work, statement is executed with LOCAL_ONE consistency level
        simpleStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
        final ResultSet resultSet = sessionN1.execute(stmt);

        final List<Item> items = Item.readItems(resultSet);

        Assert.assertEquals(1, items.size());
        final Item item = items.iterator().next();

        Assert.assertEquals(priceless.itemId, item.itemId);
        Assert.assertEquals(priceless.description, item.description);
        Assert.assertEquals(priceless.price, item.price);
        Assert.assertNull(item.price);


        final String readAllCfStmt = "READ TRANSACTIONAL TRANSACTION AS JSON '" + txStateJson + "' FROM mpptest.items ";

        final ResultSet allCf = sessionN1.execute(readAllCfStmt);
        final List<Item> allItems = Item.readItems(allCf);

        Assert.assertEquals(1, allItems.stream().filter(i -> i.equals(itemWithDescriptionAndPrice)).count());
        Assert.assertEquals(1, allItems.stream().filter(i -> i.equals(priceless)).count());
        Assert.assertEquals(1, allItems.stream().filter(i -> i.equals(itemWithJustId)).count());
    }

    private String getJson(TransactionStateDto transactionStateDto)
    {
        final ObjectMapper objectMapper = new ObjectMapper();
        try
        {
            return objectMapper.writeValueAsString(transactionStateDto);
        }
        catch (JsonProcessingException e)
        {
            throw new RuntimeException(e);
        }
    }



    @Test
    public void testWritingToPrivateMemtables() {
        Session session = getSessionN1();

        final UUID itemId = UUIDs.random();
        String itemDescription = "Super creazy awesome!";
        String price = "15 euro";

        // TODO this should be obtained using BEGIN TRANSACTION
        final UUID txId = UUIDs.timeBased();
//
        final ResultSet execute = session.execute("INSERT INTO mpptest.items (item_id, item_description, price) values (?, ?, ?) USING TRANSACTION " + txId, itemId, itemDescription, price);
        final List<Row> all = execute.all();
        all.stream().forEach(r -> {
            System.out.println("Row: " + r);

            final String ksName = r.getString("ks_name");
            final String cfName = r.getString("cf_name");
            final Long token = r.getLong("token");

            System.out.println("Keyspace name: " + ksName);
            System.out.println("ColumnFamily name: " + cfName);
            System.out.println("Token : " + token);
        });

//        final UUID itemId = UUID.fromString("55033639-59b5-4270-821a-792d68e675cc");
//        TODO [MPP] Transaction has to be initiated on server side
//        final UUID transactionId = UUIDs.timeBased();
//        String updatedItemDescription = "Book was not so great after all";
//
//        session.execute("UPDATE mpptest.items USING TRANSACTION " + transactionId + " SET item_description = '" + updatedItemDescription + "'  WHERE item_id = " + itemId);
    }

    @Test
    public void testPreparedRollback() throws Throwable {
        Session session = getSessionN1();
        final UUID txId = UUIDs.timeBased();
//        PreparedStatement statement = session.prepare(
//
//                                                     "INSERT INTO users" + "(lastname, age, city, email, firstname)"
//                                                     + "VALUES (?,?,?,?,?);");
//
        PreparedStatement statement = session.prepare("ROLLBACK TRANSACTION LOCALLY ?");
        BoundStatement boundStatement = new BoundStatement(statement);

        session.execute(boundStatement.setUUID(0, txId));
    }

}
