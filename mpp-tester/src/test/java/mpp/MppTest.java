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

import java.util.UUID;

import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.LoggingRetryPolicy;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.config.DatabaseDescriptor;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 20/01/16
 */
public class MppTest
{

    @Test
    public void testWritingToPrivateMemtables() {
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1")
                                 .withRetryPolicy(new LoggingRetryPolicy(Policies.defaultRetryPolicy()))
                                 .withPort(DatabaseDescriptor.getNativeTransportPort()).build();
        Session session = cluster.connect("mpptest");

//        final UUID itemId = UUIDs.random();
//        String itemDescription = "This book is awesome!";
//        String price = "10 euro";
//
//        session.execute("INSERT INTO mpptest.items (item_id, item_description, price) values (?, ?, ?)", itemId, itemDescription, price);

        final UUID itemId = UUID.fromString("55033639-59b5-4270-821a-792d68e675cc");
        // TODO [MPP] Transaction has to be initiated on server side
        final UUID transactionId = UUIDs.timeBased();
        String updatedItemDescription = "Book was not so great after all";

        session.execute("UPDATE mpptest.items USING TRANSACTION " + transactionId + " SET item_description = '" + updatedItemDescription + "'  WHERE item_id = " + itemId);
    }

}
