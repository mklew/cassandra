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

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.mpp.transaction.TxLog;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 06/04/16
 */
public class MpProposeResponse
{
    final boolean promised;

    final boolean rolledback;

    final TxLog txLog;

    public MpProposeResponse(boolean promised, boolean rolledback, TxLog txLog)
    {
        Preconditions.checkArgument(!(promised && rolledback), "Cannot be promised and rolled back at same time");
        this.promised = promised;
        this.rolledback = rolledback;
        this.txLog = txLog;
    }

    public static final MpProposeResponseSerializer serializer = new MpProposeResponseSerializer();

    public static class MpProposeResponseSerializer implements IVersionedSerializer<MpProposeResponse> {

        public void serialize(MpProposeResponse mpProposeResponse, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(mpProposeResponse.promised);
            out.writeBoolean(mpProposeResponse.rolledback);
            TxLog.serializer.serialize(mpProposeResponse.txLog, out, version);
        }

        public MpProposeResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            boolean promised = in.readBoolean();
            boolean rolledBack = in.readBoolean();
            TxLog txLog = TxLog.serializer.deserialize(in, version);
            return new MpProposeResponse(promised, rolledBack, txLog);
        }

        public long serializedSize(MpProposeResponse mpProposeResponse, int version)
        {
            return TypeSizes.sizeof(mpProposeResponse.promised) +
                   TypeSizes.sizeof(mpProposeResponse.rolledback) +
                    TxLog.serializer.serializedSize(mpProposeResponse.txLog, version);
        }
    }
}
