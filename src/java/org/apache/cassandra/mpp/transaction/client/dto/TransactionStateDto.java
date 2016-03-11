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

package org.apache.cassandra.mpp.transaction.client.dto;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.cassandra.mpp.transaction.client.TransactionState;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 27/01/16
 */
public class TransactionStateDto implements Serializable
{
    UUID transactionId;

    Collection<TransactionItemDto> transactionItems;

    public UUID getTransactionId()
    {
        return transactionId;
    }

    public void setTransactionId(UUID transactionId)
    {
        this.transactionId = transactionId;
    }

    public Collection<TransactionItemDto> getTransactionItems()
    {
        return transactionItems;
    }

    public void setTransactionItems(Collection<TransactionItemDto> transactionItems)
    {
        this.transactionItems = transactionItems;
    }


    public static TransactionStateDto fromTransactionState(TransactionState transactionState) {
        final TransactionStateDto transactionStateDto = new TransactionStateDto();
        transactionStateDto.setTransactionId(transactionState.getTransactionId());

        final List<TransactionItemDto> items = transactionState.getTransactionItems().stream().map(TransactionItemDto::fromTransactionItem).collect(Collectors.toList());

        transactionStateDto.setTransactionItems(items);

        return transactionStateDto;
    }
}
