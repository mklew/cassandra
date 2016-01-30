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
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.junit.Test;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.policies.LoggingRetryPolicy;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import junit.framework.Assert;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.mpp.transaction.MppTestingUtilities;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.mpp.transaction.client.dto.TransactionStateDto;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 20/01/16
 */
public class MppTest
{


//    create table mpptest.items (item_id uuid, item_description text, price text, primary key (item_id));

    private static class Item {
        final UUID itemId;

        final String description;

        final String price;

        private Item(UUID itemId, String description, String price)
        {
            this.itemId = itemId;
            this.description = description;
            this.price = price;
        }

        public static Item newItem(String description, String price) {
            return new Item(UUIDs.random(), description, price);
        }

        public static Item newItemWithDescription(String description) {
            return new Item(UUIDs.random(), description, null);
        }

        public static Item newItemWithPrice(String price) {
            return new Item(UUIDs.random(), null, price);
        }

        public static Item newItem() {
            return new Item(UUIDs.random(), null, null);
        }

        public static TransactionState persistItem(Session session, Item item, TransactionState transactionState) {

            return MppTestingUtilities.mapResultToTransactionState(persistItemInternal(session, item, transactionState));
        }

        private static ResultSet persistItemInternal(Session session, Item item, TransactionState transactionState)
        {
            if(item.description != null && item.price != null) {
                return session.execute("INSERT INTO mpptest.items (item_id, item_description, price) values (?, ?, ?) USING TRANSACTION " + transactionState.getTransactionId(), item.itemId, item.description, item.price);
            }
            else if(item.description != null) {
                return session.execute("INSERT INTO mpptest.items (item_id, item_description) values (?, ?) USING TRANSACTION " + transactionState.getTransactionId(), item.itemId, item.description);
            }
            else if(item.price != null) {
                return session.execute("INSERT INTO mpptest.items (item_id, price) values (?, ?) USING TRANSACTION " + transactionState.getTransactionId(), item.itemId, item.price);
            }
            else {
                return session.execute("INSERT INTO mpptest.items (item_id) values (?) USING TRANSACTION " + transactionState.getTransactionId(), item.itemId);
            }

        }

        public static List<Item> readItems(ResultSet resultSet)
        {
            return StreamSupport.stream(resultSet.spliterator(), false).map(row -> {

                UUID itemId = row.getUUID("item_id");
                String description = null;
                String price = null;

                if (!row.isNull("item_description"))
                {
                    description = row.getString("item_description");
                }

                if (!row.isNull("price"))
                {
                    price = row.getString("price");
                }

                return new Item(itemId, description, price);
            }).collect(Collectors.toList());
        }
    }

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

    private TransactionState beginTransaction(Session session)
    {
        return MppTestingUtilities.mapResultToTransactionState(session.execute("START TRANSACTION"));
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

    private Session getSessionN1()
    {
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1")
                                 .withRetryPolicy(new LoggingRetryPolicy(Policies.defaultRetryPolicy()))
                                 .withPort(DatabaseDescriptor.getNativeTransportPort()).build();
        return cluster.connect("mpptest");
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
