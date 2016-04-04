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
import java.util.UUID;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.mpp.transaction.paxos.MpPaxosId;
import org.apache.cassandra.mpp.transaction.paxos.MpPaxosIdImpl;
import org.apache.cassandra.serializers.UUIDSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 04/04/16
 */
public class MpPrePrepareResponse
{
    Optional<MpPaxosId> paxosId;

    public static final MpPrePrepareResponseSerializer serializer = new MpPrePrepareResponseSerializer();

    private static final int SIZE_OF_UUID = TypeSizes.sizeof(UUIDGen.getTimeUUID());

    public MpPrePrepareResponse(Optional<MpPaxosId> paxosId)
    {
        this.paxosId = paxosId;
    }

    public static MpPrePrepareResponse from(Optional<MpPaxosId> mpPaxosId)
    {
        return new MpPrePrepareResponse(mpPaxosId);
    }

    public static class MpPrePrepareResponseSerializer implements IVersionedSerializer<MpPrePrepareResponse>
    {

        public void serialize(MpPrePrepareResponse mpPrePrepareResponse, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(mpPrePrepareResponse.paxosId.isPresent());

            mpPrePrepareResponse.paxosId.ifPresent(paxosId -> {
                try
                {
                    out.write(UUIDSerializer.instance.serialize(paxosId.getPaxosId()));
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            });
        }

        public MpPrePrepareResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            final boolean isPresent = in.readBoolean();
            if (isPresent)
            {
                final UUID id = UUIDSerializer.instance.deserialize(ByteBufferUtil.read(in, SIZE_OF_UUID));
                MpPaxosIdImpl mpPaxosId = new MpPaxosIdImpl(id);
                return new MpPrePrepareResponse(Optional.of(mpPaxosId));
            }
            else
            {
                return new MpPrePrepareResponse(Optional.empty());
            }
        }

        public long serializedSize(MpPrePrepareResponse mpPrePrepareResponse, int version)
        {
            return 1 + mpPrePrepareResponse.paxosId.map(x -> TypeSizes.sizeof(x.getPaxosId())).orElse(0);
        }
    }
}
