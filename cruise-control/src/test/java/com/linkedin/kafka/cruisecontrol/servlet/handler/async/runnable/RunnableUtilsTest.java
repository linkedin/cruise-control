/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.populateRackInfoForReplicationFactorChange;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertEquals;


public class RunnableUtilsTest {
  private static final String TOPIC = "topic";
  // There are 3 nodes in the cluster that reside on 2 racks.
  private static final Node NODE_0 = new Node(0, "localhost", 100, "rack0");
  private static final Node[] NODES = {NODE_0,
                                       new Node(1, "localhost", 100, "rack1"),
                                       new Node(2, "localhost", 100, "rack1")};
  private static final Cluster CLUSTER;
  static {
    Set<PartitionInfo> partitionInfo = Collections.singleton(new PartitionInfo(TOPIC, 0, NODE_0, NODES, NODES));
    CLUSTER = new Cluster("cluster_id", new HashSet<>(Arrays.asList(NODES)), partitionInfo, Collections.emptySet(), Collections.emptySet());
  }

  @Test
  public void testPopulateRackInfoForReplicationFactorChange() {
    Map<String, List<Integer>> brokersByRack = new HashMap<>();
    Map<Integer, String> rackByBroker = new HashMap<>();
    // Expected: RuntimeException if replication factor (RF) is more than the number of brokers.
    assertThrows(RuntimeException.class, () -> populateRackInfoForReplicationFactorChange(Collections.singletonMap((short) (NODES.length + 1),
                                                                                                                   Collections.singleton(TOPIC)),
                                                                                          CLUSTER,
                                                                                          false,
                                                                                          brokersByRack,
                                                                                          rackByBroker));

    // Expected: RuntimeException if RF is more than the number of racks and rack-awareness check is not skipped.
    assertThrows(RuntimeException.class, () -> populateRackInfoForReplicationFactorChange(Collections.singletonMap((short) NODES.length,
                                                                                                                   Collections.singleton(TOPIC)),
                                                                                          CLUSTER,
                                                                                          false,
                                                                                          brokersByRack,
                                                                                          rackByBroker));

    // Expected: No failures if RF is more than the number of racks -- but less than the total number of brokers, and
    // rack-awareness check is skipped.
    populateRackInfoForReplicationFactorChange(Collections.singletonMap((short) NODES.length, Collections.singleton(TOPIC)),
                                               CLUSTER,
                                               true,
                                               brokersByRack,
                                               rackByBroker);
    assertEquals(2, brokersByRack.size());
    assertEquals(NODES.length, rackByBroker.size());
  }
}
