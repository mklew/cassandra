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
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.mpp.transaction.internal.MppHint;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 05/04/16
 */
public class MpCommitWithHints
{
    private MpCommit commit;

    private List<MppHint> hints;

    public final static MpCommitWithHintsSerializer serializer = new MpCommitWithHintsSerializer();

    public MpCommitWithHints(MpCommit commit, List<MppHint> hints)
    {
        this.commit = commit;
        this.hints = hints;
    }

    public MpCommit getCommit()
    {
        return commit;
    }

    public List<MppHint> getHints()
    {
        return hints;
    }

    public static class MpCommitWithHintsSerializer implements IVersionedSerializer<MpCommitWithHints> {

        public void serialize(MpCommitWithHints mpCommitWithHints, DataOutputPlus out, int version) throws IOException
        {
            MpCommit.serializer.serialize(mpCommitWithHints.commit, out, version);

            /* serialize size of transaction items */
            int size = mpCommitWithHints.hints.size();
            out.writeInt(size);

            assert size > 0;

            for(MppHint hint : mpCommitWithHints.hints) {
                MppHint.serializer.serialize(hint, out, version);
            }
        }

        public MpCommitWithHints deserialize(DataInputPlus in, int version) throws IOException
        {
            MpCommit commit = MpCommit.serializer.deserialize(in, version);
            final int size = in.readInt();

            assert size > 0;
            List<MppHint> hints = new ArrayList<>(size);
            for (int i = 0; i < size; ++i) {
                MppHint hint = MppHint.serializer.deserialize(in, version);
                hints.add(hint);
            }

            return new MpCommitWithHints(commit, hints);
        }

        public long serializedSize(MpCommitWithHints mpCommitWithHints, int version)
        {
            int size = 0;

            size += MpCommit.serializer.serializedSize(mpCommitWithHints.commit, version);
            size += TypeSizes.sizeof(mpCommitWithHints.hints.size());
            for (MppHint hint : mpCommitWithHints.hints) {
                size += MppHint.serializer.serializedSize(hint, version);
            }
            return size;
        }
    }
}
