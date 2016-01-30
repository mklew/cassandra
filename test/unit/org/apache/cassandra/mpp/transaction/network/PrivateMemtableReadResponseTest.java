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

package org.apache.cassandra.mpp.transaction.network;

import java.util.Optional;

import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.mpp.transaction.network.messages.PrivateMemtableReadResponse;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Tables;

import static org.apache.cassandra.mpp.transaction.internal.TransationDataImplTest.getClustering;
import static org.apache.cassandra.mpp.transaction.internal.TransationDataImplTest.getColumnDef;
import static org.apache.cassandra.mpp.transaction.internal.TransationDataImplTest.makeCf;
import static org.apache.cassandra.mpp.transaction.internal.TransationDataImplTest.makeCfMetaData;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 30/01/16
 */
public class PrivateMemtableReadResponseTest extends CQLTester
{

    final int version = MessagingService.current_version;

    @Test
    public void shouldSerializeAndDeserializeEmptyPartitionUpdate() throws Exception {
        final PrivateMemtableReadResponse response = new PrivateMemtableReadResponse(Optional.empty());
        final PrivateMemtableReadResponse deserialized = serializeAndDeserialize(response);

        Assert.assertFalse(deserialized.getPartitionUpdateOpt().isPresent());
    }

    private PrivateMemtableReadResponse serializeAndDeserialize(PrivateMemtableReadResponse response) throws java.io.IOException
    {
        final DataOutputBuffer out = new DataOutputBuffer();

        PrivateMemtableReadResponse.serializer.serialize(response, out, version);

        DataInputPlus in = new DataInputBuffer(out.buffer(), true);

        return PrivateMemtableReadResponse.serializer.deserialize(in, version);
    }

    @Test
    public void shouldSerializeAndDeserializeRealPartitionUpdate() throws Exception {
        CFMetaData cf1MetaData = makeCfMetaData("ks1", "cf1");
        Keyspace.setInitialized();
        Schema.instance.addKeyspace(KeyspaceMetadata.create("ks1", KeyspaceParams.simple(1), Tables.of(cf1MetaData)));

        // For same partition key
        String partitionKey = "k";
        final String ck1 = "ck1";
        PartitionUpdate partitionUpdate = makeCf(cf1MetaData, partitionKey, ck1, "k1v1", null);

        final PrivateMemtableReadResponse response = new PrivateMemtableReadResponse(Optional.of(partitionUpdate));

        final PrivateMemtableReadResponse deserialized = serializeAndDeserialize(response);

        Assert.assertTrue(deserialized.getPartitionUpdateOpt().isPresent());

        final PartitionUpdate puDeserialized = deserialized.getPartitionUpdateOpt().get();


        final String c1 = UTF8Type.instance.compose(puDeserialized.getRow(getClustering(ck1)).getCell(getColumnDef(cf1MetaData, "c1")).value());

        final Cell c21 = puDeserialized.getRow(getClustering(ck1)).getCell(getColumnDef(cf1MetaData, "c2"));
        Assert.assertNull(c21);

        Assert.assertEquals("k1v1", c1);

    }


}
