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

package mpp;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import com.datastax.driver.core.Session;
import junit.framework.Assert;
import org.apache.cassandra.tools.NodeProbe;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 20/02/16
 */
public class ClusterIsUpTest extends BaseClusterTest
{

    @Test
    public void shouldCreateSessionWithEachNode() {
        Session node1 = getSessionN1();
        Assert.assertNotNull(node1);
        Session node2 = getSessionN2();
        Assert.assertNotNull(node2);
        Session node3 = getSessionN3();
        Assert.assertNotNull(node3);
    }

    @Test
    public void nodeProbesShouldWork() {
        NodeProbe node1 = getNodeProbe1();
        Assert.assertNotNull(node1);
        NodeProbe node2 = getNodeProbe2();
        Assert.assertNotNull(node2);
        NodeProbe node3 = getNodeProbe3();
        Assert.assertNotNull(node3);

        Set<String> ids = new HashSet<>();
        ids.add(node1.getLocalHostId());
        ids.add(node2.getLocalHostId());
        ids.add(node3.getLocalHostId());

        Assert.assertEquals("Some node probes do not work", 3, ids.size());
    }

}
