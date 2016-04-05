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

package org.apache.cassandra.mpp.transaction.paxos;

import java.util.UUID;

import com.google.common.base.Preconditions;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 11/03/16
 */
public class MpPaxosIdImpl implements MpPaxosId
{
    private final UUID mpPaxosId;

    public MpPaxosIdImpl(UUID mpPaxosId)
    {
        Preconditions.checkNotNull(mpPaxosId);
        this.mpPaxosId = mpPaxosId;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MpPaxosIdImpl mpPaxosId1 = (MpPaxosIdImpl) o;

        if (!mpPaxosId.equals(mpPaxosId1.mpPaxosId)) return false;

        return true;
    }

    public int hashCode()
    {
        return mpPaxosId.hashCode();
    }

    @Override
    public UUID getPaxosId()
    {
        return mpPaxosId;
    }

    public String toString()
    {
        return "MpPaxosId[" + mpPaxosId + ']';
    }
}
