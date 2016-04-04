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

package org.apache.cassandra.service.mppaxos;


import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.BooleanSerializer;

public class MpProposeVerbHandler implements IVerbHandler<MpCommit>
{
    public void doVerb(MessageIn<MpCommit> message, int id)
    {
        Boolean response = MpPaxosState.propose(message.payload);
        MessageOut<Boolean> reply = new MessageOut<Boolean>(MessagingService.Verb.REQUEST_RESPONSE, response, BooleanSerializer.serializer);
        MessagingService.instance().sendReply(reply, id, message.from);
    }
}
