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

package org.apache.cassandra.db;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;

import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tracing.Tracing;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 23/01/16
 */
public class PrivateMemtableWriteVerbHandler implements IVerbHandler<TransactionalMutation>
{
    public void doVerb(MessageIn<TransactionalMutation> message, int id) throws IOException
    {
        // Check if there were any forwarding headers in this message
        byte[] from = message.parameters.get(Mutation.FORWARD_FROM);
        InetAddress replyTo;
        if (from == null)
        {
            replyTo = message.from;
            byte[] forwardBytes = message.parameters.get(Mutation.FORWARD_TO);
            if (forwardBytes != null)
                forwardToLocalNodes(message.payload, message.verb, forwardBytes, message.from);
        }
        else
        {
            replyTo = InetAddress.getByAddress(from);
        }

        try
        {
//            if (message.version < MessagingService.VERSION_30 && LegacyBatchlogMigrator.isLegacyBatchlogMutation(message.payload))
//                LegacyBatchlogMigrator.handleLegacyMutation(message.payload);
//            else
            final TransactionItem applied = message.payload.apply();
            // TODO [MPP] This transactionItem or something else could be used as respones,
            // TODO but because coordinator can create these transaction items just by knowing that request succeeded there is no point to return that in response

            Tracing.trace("Enqueuing response to {}", replyTo);
            // TODO [MPP] Here I'd have to use different WriteResponse who creates different message usign results of payload.apply()
            MessagingService.instance().sendReply(WriteResponse.createMessage(), id, replyTo);
        }
        catch (WriteTimeoutException wto)
        {
            Tracing.trace("Payload application resulted in WriteTimeout, not replying");
        }
    }

    /**
     * Older version (< 1.0) will not send this message at all, hence we don't
     * need to check the version of the data.
     */
    private static void forwardToLocalNodes(TransactionalMutation mutation, MessagingService.Verb verb, byte[] forwardBytes, InetAddress from) throws IOException
    {
        try (DataInputStream in = new DataInputStream(new FastByteArrayInputStream(forwardBytes)))
        {
            int size = in.readInt();

            // tell the recipients who to send their ack to
            MessageOut<TransactionalMutation> message = new MessageOut<>(verb, mutation, TransactionalMutation.serializer).withParameter(Mutation.FORWARD_FROM, from.getAddress());
            // Send a message to each of the addresses on our Forward List
            for (int i = 0; i < size; i++)
            {
                InetAddress address = CompactEndpointSerializationHelper.deserialize(in);
                int id = in.readInt();
                Tracing.trace("Enqueuing forwarded write to {}", address);
                MessagingService.instance().sendOneWay(message, id, address);
            }
        }
    }
}
