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

import java.net.InetAddress;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.mpp.transaction.MppCQLTester;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.Util.token;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 27/01/16
 */
public class ReadTransactiondataServiceImplTest extends MppCQLTester
{

    public final static String ONE = "1";
    public final static String SIX = "6";

    static TokenMetadata tmd;

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        tmd = StorageService.instance.getTokenMetadata();
        tmd.updateNormalToken(token(ONE), InetAddress.getByName("127.0.0.1"));
        tmd.updateNormalToken(token(SIX), InetAddress.getByName("127.0.0.6"));
    }

    @Test
    public void shouldIdentifyLocalNodeAsReplicaOfTransactionItem() throws Throwable {
        // given
        TransactionState transactionState = startTransaction();
        createMppTestCf1();

        transactionState = transactionState.merge(txInsertToCf1(transactionState, 10, "a"));

        // when
        final ReadTransactionDataServiceImpl service = mppExtensionServices.getMppModule().getReadTransactionDataServiceImpl();


        final Stream<ReadTransactionDataServiceImpl.TransactionItemWithAddresses> r = service.identifyTransactionItemsOwnedByThisNode(transactionState);
        final List<ReadTransactionDataServiceImpl.TransactionItemWithAddresses> collect = r.collect(Collectors.toList());

        Assert.assertEquals(1, collect.size());
    }

}
