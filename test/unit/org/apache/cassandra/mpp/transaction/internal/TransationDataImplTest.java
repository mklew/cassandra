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

package org.apache.cassandra.mpp.transaction.internal;

import java.util.Optional;
import java.util.UUID;

import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.mpp.transaction.TransactionId;
import org.apache.cassandra.mpp.transaction.client.TransactionStateUtils;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 29/01/16
 */
public class TransationDataImplTest
{

    public static PartitionUpdate makeCf(CFMetaData metadata, String partitionKey, String clusteringKey, String columnValue1, String columnValue2)
    {
        Row.Builder builder = BTreeRow.unsortedBuilder(FBUtilities.nowInSeconds());
        builder.newRow(getClustering(clusteringKey));
        long ts = FBUtilities.timestampMicros();
        if (columnValue1 != null)
            builder.addCell(BufferCell.live(metadata, metadata.getColumnDefinition(bytes("c1")), ts, bytes(columnValue1)));
        if (columnValue2 != null)
            builder.addCell(BufferCell.live(metadata, metadata.getColumnDefinition(bytes("c2")), ts, bytes(columnValue2)));

        return PartitionUpdate.singleRowUpdate(metadata, Util.dk(partitionKey), builder.build());
    }

    public static Clustering getClustering(String clusteringKey)
    {
        return new Clustering(UTF8Type.instance.decompose(clusteringKey));
    }

    public static CFMetaData makeCfMetaData(String ks, String cf)
    {
        CFMetaData metadata = CFMetaData.Builder.create(ks, cf)
                                                .addPartitionKey("pkey", UTF8Type.instance)
                                                .addClusteringColumn("ck", UTF8Type.instance)
                                                .addRegularColumn("c1", UTF8Type.instance)
                                                .addRegularColumn("c2", UTF8Type.instance)
                                                .build();

        return metadata;
    }

    private static class PartitionUpdateLookup {

        private final String ksName;

        private final UUID cfId;

        private final Token token;

        public PartitionUpdateLookup(String ksName, UUID cfId, Token token)
        {
            this.ksName = ksName;
            this.cfId = cfId;
            this.token = token;
        }
    }

    @Test
    public void testShouldReturnEmptyPartitionUpdateIfItDoesntHaveIt() {
        CFMetaData cf1MetaData = makeCfMetaData("ks1", "cf1");
        // For same partition key
        String partitionKey = "k";
        final String ck1 = "ck1";
        PartitionUpdate cf1 = makeCf(cf1MetaData, partitionKey, ck1, "k1v1", null);
        Mutation m1 = new Mutation("ks1", cf1.partitionKey()).add(cf1);

        final TransactionId transactionId = TransactionStateUtils.newTransactionState().id();

        final TransactionDataImpl privateData = new TransactionDataImpl(transactionId);

        privateData.addMutation(m1);

        // when reading for something that does not exist
        CFMetaData cf2MetaData = makeCfMetaData("ks1", "cf2");

        final Optional<PartitionUpdate> partitionUpdate = privateData.readData("ks1", cf2MetaData.cfId, Util.token(partitionKey));

        Assert.assertFalse(partitionUpdate.isPresent());
    }

    @Test
    public void testOperationsScenario() {
        CFMetaData cf1MetaData = makeCfMetaData("ks1", "cf1");
        CFMetaData cf2MetaData = makeCfMetaData("ks1", "cf2");

        // For same partition key
        String partitionKey = "k";
        final String ck1 = "ck1";
        PartitionUpdate cf1 = makeCf(cf1MetaData, partitionKey, ck1, "k1v1", null);
        PartitionUpdate cf2 = makeCf(cf2MetaData, partitionKey, "ck2", "k2v1", null);

        // Create two mutations for different column familites
        Mutation m1 = new Mutation("ks1", cf1.partitionKey()).add(cf1);
        Mutation m2 = new Mutation("ks1", cf2.partitionKey()).add(cf2);


//        PartitionUpdate cf2 = makeCf(cf1MetaData, "k2", "k2v1", null);



        final TransactionId transactionId = TransactionStateUtils.newTransactionState().id();

        final TransactionDataImpl privateData = new TransactionDataImpl(transactionId);

        privateData.addMutation(m1);

        privateData.addMutation(m2);

        Assert.assertEquals("Mutations should be merged into single because updates are for same partition key", 1, privateData.getMutations().size());

        final Mutation singleMutation = privateData.getMutations().iterator().next();

        final PartitionUpdate partitionUpdateCf1 = singleMutation.get(cf1MetaData);
        final PartitionUpdate partitionUpdateCf2 = singleMutation.get(cf2MetaData);

        Assert.assertEquals("PartitionUpdates keys are always the same", partitionUpdateCf1.partitionKey(), partitionUpdateCf2.partitionKey());

        final Cell cell = partitionUpdateCf1.getRow(getClustering(ck1)).getCell(getColumnDef(cf1MetaData, "c1"));
        Assert.assertEquals("k1v1", UTF8Type.instance.compose(cell.value()));

        // TransactionItem is defined per keyspace per column family per token -- token is based on partition key

        // Therefore there should be only one PartitionUpdate defined for TransactionItem

        final Optional<PartitionUpdate> partitionUpdateOpt = privateData.readData("ks1", cf1MetaData.cfId, Util.token(partitionKey));

        Assert.assertTrue("PartitionUpdate should be present", partitionUpdateOpt.isPresent());
        Assert.assertEquals(1, partitionUpdateOpt.get().rowCount());
        // Another insert with same partitioning key should be merged as PartitionUpdate with two rows.
        final String ck3 = "ck3";
        final PartitionUpdate pu3 = makeCf(cf1MetaData, partitionKey, ck3, "k3v11", "k3v22");
        Mutation m3 = new Mutation("ks1", pu3.partitionKey()).add(pu3);

        privateData.addMutation(m3);

        final PartitionUpdate partitionUpdateWithPu3 = privateData.readData("ks1", cf1MetaData.cfId, Util.token(partitionKey)).get();
        Assert.assertEquals("Should have two rows for same partitioning key", 2, partitionUpdateWithPu3.rowCount());

        Assert.assertEquals("k3v22", UTF8Type.instance.compose(partitionUpdateWithPu3.getRow(getClustering(ck3)).getCell(getColumnDef(cf1MetaData, "c2")).value()));
    }

    public static ColumnDefinition getColumnDef(CFMetaData cf1MetaData, String c)
    {
        return cf1MetaData.getColumnDefinition(UTF8Type.instance.decompose(c));
    }
}
