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

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.mpp.transaction.MppTestingUtilities;
import org.apache.cassandra.mpp.transaction.client.TransactionState;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 20/02/16
 */
public class MppTestSchemaHelpers
{


    public static final ConsistencyLevel LOCAL_TRANSACTIONAL = ConsistencyLevel.LOCAL_TRANSACTIONAL;

    public static class UserItems {

        final String userName;

        final UUID itemId;

        final String itemDescription;


        public UserItems(String userName, UUID itemId)
        {
            assert userName != null;
            assert itemId != null;
            this.userName = userName;
            this.itemId = itemId;
            this.itemDescription = null;
        }

        public UserItems(String userName, UUID itemId, String itemDescription)
        {
            assert userName != null;
            assert itemId != null;

            this.userName = userName;
            this.itemId = itemId;
            this.itemDescription = itemDescription;
        }

        public static UserItems newUserItem(String userName, Item item) {
            return new UserItems(userName, item.itemId, item.description);
        }

        public static TransactionState persistUserItem(Session session, UserItems userItem, TransactionState transactionState) {

            return MppTestingUtilities.mapResultToTransactionState(persistUserItemInternal(session, userItem, transactionState));
        }

        private static ResultSet persistUserItemInternal(Session session, UserItems userItem, TransactionState transactionState)
        {
            if(userItem.itemDescription != null ) {
                SimpleStatement simpleStatement = new SimpleStatement("INSERT INTO mpptest.user_items (user_name, item_id, item_description) values (?, ?, ?) USING TRANSACTION " + transactionState.getTransactionId(), userItem.userName, userItem.itemId, userItem.itemDescription);
                simpleStatement.setConsistencyLevel(LOCAL_TRANSACTIONAL);
                return session.execute(simpleStatement);
            }
            else  {
                SimpleStatement simpleStatement = new SimpleStatement("INSERT INTO mpptest.user_items (user_name, item_id) values (?, ?) USING TRANSACTION " + transactionState.getTransactionId(), userItem.userName, userItem.itemId);
                simpleStatement.setConsistencyLevel(LOCAL_TRANSACTIONAL);
                return session.execute(simpleStatement);
            }
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            UserItems userItems = (UserItems) o;

            if (itemDescription != null ? !itemDescription.equals(userItems.itemDescription) : userItems.itemDescription != null)
                return false;
            if (!itemId.equals(userItems.itemId)) return false;
            if (!userName.equals(userItems.userName)) return false;

            return true;
        }

        public int hashCode()
        {
            int result = userName.hashCode();
            result = 31 * result + itemId.hashCode();
            result = 31 * result + (itemDescription != null ? itemDescription.hashCode() : 0);
            return result;
        }
    }

    public static class Item {
        final UUID itemId;

        final String description;

        final String price;

        private Item(UUID itemId, String description, String price)
        {
            this.itemId = itemId;
            this.description = description;
            this.price = price;
        }

        public Item copyWithDescription(String description) {
            return new Item(itemId, description, price);
        }

        public Item copyWithPrice(String price) {
            return new Item(itemId, description, price);
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

        public static Item persistItemWithoutTx(Session session, Item item) {
            SimpleStatement simpleStatement = new SimpleStatement("INSERT INTO mpptest.items (item_id, item_description, price) values (?, ?, ?)", item.itemId, item.description, item.price);
            simpleStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
            session.execute(simpleStatement);
            return findItemById(item.itemId, session);
        }

        public static TransactionState persistItem(Session session, Item item, TransactionState transactionState) {

            return MppTestingUtilities.mapResultToTransactionState(persistItemInternal(session, item, transactionState));
        }

        private static ResultSet persistItemInternal(Session session, Item item, TransactionState transactionState)
        {
            if(item.description != null && item.price != null) {
                SimpleStatement simpleStatement = new SimpleStatement("INSERT INTO mpptest.items (item_id, item_description, price) values (?, ?, ?) USING TRANSACTION " + transactionState.getTransactionId(), item.itemId, item.description, item.price);
                simpleStatement.setConsistencyLevel(LOCAL_TRANSACTIONAL);
                return session.execute(simpleStatement);
            }
            else if(item.description != null) {
                SimpleStatement simpleStatement = new SimpleStatement("INSERT INTO mpptest.items (item_id, item_description) values (?, ?) USING TRANSACTION " + transactionState.getTransactionId(), item.itemId, item.description);
                simpleStatement.setConsistencyLevel(LOCAL_TRANSACTIONAL);
                return session.execute(simpleStatement);
            }
            else if(item.price != null) {
                SimpleStatement simpleStatement = new SimpleStatement("INSERT INTO mpptest.items (item_id, price) values (?, ?) USING TRANSACTION " + transactionState.getTransactionId(), item.itemId, item.price);
                simpleStatement.setConsistencyLevel(LOCAL_TRANSACTIONAL);
                return session.execute(simpleStatement);
            }
            else {
                SimpleStatement simpleStatement = new SimpleStatement("INSERT INTO mpptest.items (item_id) values (?) USING TRANSACTION " + transactionState.getTransactionId(), item.itemId);
                simpleStatement.setConsistencyLevel(LOCAL_TRANSACTIONAL);
                return session.execute(simpleStatement);
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

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Item item = (Item) o;

            if (description != null ? !description.equals(item.description) : item.description != null) return false;
            if (!itemId.equals(item.itemId)) return false;
            if (price != null ? !price.equals(item.price) : item.price != null) return false;

            return true;
        }

        public int hashCode()
        {
            int result = itemId.hashCode();
            result = 31 * result + (description != null ? description.hashCode() : 0);
            result = 31 * result + (price != null ? price.hashCode() : 0);
            return result;
        }

        public static Item findItemById(UUID itemId, Session session)
        {
            ResultSet query = session.execute("SELECT * FROM mpptest.items WHERE item_id = ? ", itemId);
            return readItems(query).iterator().next();
        }

        public String toString()
        {
            return "Item{" +
                   "itemId=" + itemId +
                   ", description='" + description + '\'' +
                   ", price='" + price + '\'' +
                   '}';
        }
    }

}
