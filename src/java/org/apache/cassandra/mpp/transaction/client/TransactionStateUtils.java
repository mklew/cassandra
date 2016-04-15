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

package org.apache.cassandra.mpp.transaction.client;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.SystemClock;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.mpp.transaction.TransactionId;
import org.apache.cassandra.mpp.transaction.TransactionTimeUUID;
import org.apache.cassandra.mpp.transaction.client.dto.TransactionItemDto;
import org.apache.cassandra.mpp.transaction.client.dto.TransactionStateDto;
import org.apache.cassandra.utils.UUIDGen;
import org.joda.time.DateTime;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 26/11/15
 */
public class TransactionStateUtils
{

    /**
     * Warning: should be called from inside C*, because of time synchronization between nodes.
     *
     * @return new empty {@link TransactionState} with ID assigned
     */
    public static TransactionState newTransactionState()
    {
        return new TransactionState(UUIDGen.getTimeUUID(SystemClock.getCurrentTimeMillis()), Collections.emptyList());
    }

    public static TransactionState recreateTransactionState(UUID id, Collection<TransactionItem> transactionItems)
    {
        return new TransactionState(id, transactionItems);
    }

    public static TransactionState recreateTransactionState(TransactionId id, Collection<TransactionItem> transactionItems)
    {
        // TODO [MPP] Get rid of casting
        return recreateTransactionState(((TransactionTimeUUID) id).getId(), transactionItems);
    }

    public static TransactionState fromId(UUID id) {
        return recreateTransactionState(id, Collections.emptyList());
    }

    public static TransactionState compose(TransactionStateDto dto)
    {
        final UUID transactionId = dto.getTransactionId();

        final List<TransactionItem> transactionItems = dto.getTransactionItems()
                                                          .stream()
                                                          .map(composeItem())
                                                          .collect(Collectors.toList());

        return recreateTransactionState(transactionId, transactionItems);
    }

    public static TransactionItem composeItem(TransactionItemDto transactionItemDto)
    {
        return composeItem().apply(transactionItemDto);
    }

    private static Function<TransactionItemDto, TransactionItem> composeItem()
    {
        return ti -> new TransactionItem(new Murmur3Partitioner.LongToken(ti.getToken()),
                                       ti.getKsName(),
                                       ti.getCfName());
    }

    public static UUID createReadOnlyTransactionIdRaw() {
        return UUIDs.startOf(new DateTime().plusYears(1).plusDays(1).getMillis());
    }

    public static TransactionState createReadOnlyTransaction(Token token, String ksName, String cfName)
    {
        TransactionItem transactionItem = new TransactionItem(token, ksName, cfName);

        UUID uuid = createReadOnlyTransactionIdRaw();

        TransactionState transactionState = new TransactionState(uuid, Collections.singletonList(transactionItem));
        assert transactionState.isReadTransaction();

        return transactionState;
    }
}
