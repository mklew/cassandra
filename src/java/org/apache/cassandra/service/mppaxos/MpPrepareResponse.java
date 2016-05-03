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

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.mpp.transaction.TxLog;

public class MpPrepareResponse
{
    public static final PrepareResponseSerializer serializer = new PrepareResponseSerializer();

    public final boolean promised;

    public final boolean rolledBack;

    public final TxLog txLog;

    /*
     * To maintain backward compatibility (see #6023), the meaning of inProgressCommit is a bit tricky.
     * If promised is true, then that's the last accepted commit. If promise is false, that's just
     * the previously promised ballot that made us refuse this one.
     */
    public final MpCommit inProgressCommit;
    public final MpCommit mostRecentCommit;

    public MpPrepareResponse(boolean promised, boolean rolledBack, MpCommit inProgressCommit, MpCommit mostRecentCommit, TxLog txLog)
    {
//        assert inProgressCommit.update.partitionKey().equals(mostRecentCommit.update.partitionKey());
//        assert inProgressCommit.update.metadata() == mostRecentCommit.update.metadata();

        this.promised = promised;
        this.rolledBack = rolledBack;
        this.mostRecentCommit = mostRecentCommit;
        this.inProgressCommit = inProgressCommit;
        this.txLog = txLog;
    }

    @Override
    public String toString()
    {
        return String.format("PrepareResponse(%s, %s, %s)", promised, mostRecentCommit, inProgressCommit);
    }

    public static class PrepareResponseSerializer implements IVersionedSerializer<MpPrepareResponse>
    {
        public void serialize(MpPrepareResponse response, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(response.promised);
            out.writeBoolean(response.rolledBack);
            MpCommit.serializer.serialize(response.inProgressCommit, out, version);
            MpCommit.serializer.serialize(response.mostRecentCommit, out, version);
            TxLog.serializer.serialize(response.txLog, out, version);
        }

        public MpPrepareResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            boolean success = in.readBoolean();
            boolean rolledBack = in.readBoolean();
            MpCommit inProgress = MpCommit.serializer.deserialize(in, version);
            MpCommit mostRecent = MpCommit.serializer.deserialize(in, version);
            TxLog txLog = TxLog.serializer.deserialize(in, version);
            return new MpPrepareResponse(success, rolledBack, inProgress, mostRecent, txLog);
        }

        public long serializedSize(MpPrepareResponse response, int version)
        {
            return TypeSizes.sizeof(response.promised)
                      + MpCommit.serializer.serializedSize(response.inProgressCommit, version)
                      + MpCommit.serializer.serializedSize(response.mostRecentCommit, version)
                      + TxLog.serializer.serializedSize(response.txLog, version);
        }
    }
}
