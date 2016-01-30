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

package org.apache.cassandra.mpp.transaction.network.messages;

import java.io.IOException;
import java.util.Optional;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.mpp.MppServicesLocator;
import org.apache.cassandra.mpp.transaction.NodeContext;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.mpp.transaction.network.MppRequestMessage;
import org.apache.cassandra.mpp.transaction.network.MppResponseMessage;
import org.apache.cassandra.mpp.transaction.serialization.TransactionStateSerializer;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

/**
 * Naming comes from {@link org.apache.cassandra.db.ReadCommand}.
 * <p>
 * Response for this is {@link org.apache.cassandra.mpp.transaction.network.messages.PrivateMemtableReadResponse}
 *
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 30/01/16
 */
public class PrivateMemtableReadCommand implements MppRequestMessage
{

    private final TransactionState transactionStateWithSingleTransactionItem;

    public static final IVersionedSerializer<PrivateMemtableReadCommand> serializer = new Serializer();

    public PrivateMemtableReadCommand(TransactionState transactionStateWithSingleTransactionItem)
    {
        Preconditions.checkArgument(transactionStateWithSingleTransactionItem.getTransactionItems().size() == 1,
                                    "Should have only single transaction item, but had %s",
                                    transactionStateWithSingleTransactionItem.getTransactionItems().toString());
        this.transactionStateWithSingleTransactionItem = transactionStateWithSingleTransactionItem;
    }

    /**
     * This is called if this read command is treated as mpp request message.
     *
     * @param context
     * @return
     */
    public MppResponseMessage executeInLocalContext(NodeContext context)
    {
        throw new RuntimeException(" NOT IMPLEMENTED FOR MPP NETWORK SERVICE");
        // TODO [MPP] Maybe implement it later.
    }

    public PrivateMemtableReadResponse execute()
    {
        final Optional<PartitionUpdate> partitionUpdateOpt = MppServicesLocator.getInstance().readSingleTransactionItem(transactionStateWithSingleTransactionItem);

        return new PrivateMemtableReadResponse(partitionUpdateOpt);
    }

    public MessageOut<PrivateMemtableReadCommand> createMessage()
    {
        return new MessageOut<>(MessagingService.Verb.PRIVATE_MEMTABLE_READ, this, serializer);
    }

    private static class Serializer implements IVersionedSerializer<PrivateMemtableReadCommand>
    {

        public void serialize(PrivateMemtableReadCommand privateMemtableReadCommand, DataOutputPlus out, int version) throws IOException
        {
            TransactionStateSerializer.instance.serialize(privateMemtableReadCommand.transactionStateWithSingleTransactionItem, out, version);
        }

        public PrivateMemtableReadCommand deserialize(DataInputPlus in, int version) throws IOException
        {
            final TransactionState transactionState = TransactionStateSerializer.instance.deserialize(in, version);
            return new PrivateMemtableReadCommand(transactionState);
        }

        public long serializedSize(PrivateMemtableReadCommand privateMemtableReadCommand, int version)
        {
            return TransactionStateSerializer.instance.serializedSize(privateMemtableReadCommand.transactionStateWithSingleTransactionItem, version);
        }
    }
}
