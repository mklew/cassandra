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

import org.junit.Test;

import org.apache.cassandra.tools.NodeProbe;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 17/02/16
 */
public class NodeProbeTest
{
    @Test
    public void testThatNodeProbeCanConnectToNodesInCcmCluster() throws Throwable {
        // given ccm cluster is started.

        NodeProbe node1 = new NodeProbe("127.0.0.1", 7100);
        NodeProbe node2 = new NodeProbe("127.0.0.1", 7200);
        NodeProbe node3 = new NodeProbe("127.0.0.1", 7300);
        System.out.println(node1.getLocalHostId());




        System.out.println(node3.getLocalHostId());
        Thread.sleep(500);

        System.out.println(node2.getLocalHostId());

    }
}
