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

import java.io.IOException;
import java.util.Optional;

import org.apache.cassandra.mpp.MppServicesLocator;
import org.apache.cassandra.mpp.transaction.TxLog;
import org.apache.cassandra.mpp.transaction.paxos.MpPaxosId;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.Pair;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 04/04/16
 */
public class MpPrePrepareVerbHandler implements IVerbHandler<MpPrePrepare>
{
    public void doVerb(MessageIn<MpPrePrepare> message, int id) throws IOException
    {
        MpPrePrepare prePrepare = message.payload;

        Pair<Optional<MpPaxosId>, TxLog> mpPaxosId = MppServicesLocator.getInstance().prePrepareMultiPartitionPaxos(prePrepare);
        MpPrePrepareResponse response = MpPrePrepareResponse.from(mpPaxosId.left, mpPaxosId.right);
        MessageOut<MpPrePrepareResponse> reply = new MessageOut<>(MessagingService.Verb.REQUEST_RESPONSE, response, MpPrePrepareResponse.serializer);
        MessagingService.instance().sendReply(reply, id, message.from);
    }
}
