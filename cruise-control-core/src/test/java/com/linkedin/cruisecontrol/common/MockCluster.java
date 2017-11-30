/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.common;

import com.linkedin.cruisecontrol.exception.ModelInputException;
import java.util.HashMap;
import java.util.Map;


public class MockCluster {
  private MockCluster() {

  }

  public static MockNodeImpl createMockCluster() throws ModelInputException {
    // Generate a cluster root node.
    Map<String, Object> clusterTags = new HashMap<>();
    clusterTags.put("someValidKey", 0);
    clusterTags.put("anotherValidKey", 0);
    clusterTags.put("location", "Sunnyvale");
    MockNodeImpl clusterNode = new MockNodeImpl(0, "root", clusterTags, null);

    // Generate 2 racks under the cluster node.
    //********************************************************************************************//
    Map<String, Object> rack1Tags = new HashMap<>();
    rack1Tags.put("isPreferred", true);
    MockNodeImpl rack1Node = new MockNodeImpl(1, "r1", rack1Tags, clusterNode);

    Map<String, Object> rack2Tags = new HashMap<>();
    rack2Tags.put("isPreferred", false);
    MockNodeImpl rack2Node = new MockNodeImpl(1, "r2", rack2Tags, clusterNode);

    // Add children to cluster.
    clusterNode.addChild(rack1Node);
    clusterNode.addChild(rack2Node);

    // Generate 2 hosts under each rack node -- i.e. 4 nodes in total.
    //********************************************************************************************//
    Map<String, Object> host1Tags = new HashMap<>();
    host1Tags.put("isBlacklisted", false);
    MockNodeImpl host1Node = new MockNodeImpl(2, "h1", host1Tags, rack1Node);

    Map<String, Object> host2Tags = new HashMap<>();
    host2Tags.put("isBlacklisted", false);
    MockNodeImpl host2Node = new MockNodeImpl(2, "h2", host2Tags, rack1Node);

    Map<String, Object> host3Tags = new HashMap<>();
    host3Tags.put("isBlacklisted", false);
    MockNodeImpl host3Node = new MockNodeImpl(2, "h3", host3Tags, rack2Node);

    Map<String, Object> host4Tags = new HashMap<>();
    host4Tags.put("isBlacklisted", false);
    MockNodeImpl host4Node = new MockNodeImpl(2, "h4", host4Tags, rack2Node);

    // Add children to racks.
    rack1Node.addChild(host1Node);
    rack1Node.addChild(host2Node);
    rack2Node.addChild(host3Node);
    rack2Node.addChild(host4Node);

    // Generate a process under each host node -- i.e. 4 processes in total.
    //********************************************************************************************//
    Map<String, Object> process1Tags = new HashMap<>();
    process1Tags.put("status", "dead");
    MockNodeImpl process1Node = new MockNodeImpl(3, "p1", process1Tags, host1Node);

    Map<String, Object> process2Tags = new HashMap<>();
    process2Tags.put("status", "alive");
    MockNodeImpl process2Node = new MockNodeImpl(3, "p2", process2Tags, host2Node);

    Map<String, Object> process3Tags = new HashMap<>();
    process3Tags.put("status", "new");
    MockNodeImpl process3Node = new MockNodeImpl(3, "p3", process3Tags, host3Node);

    Map<String, Object> process4Tags = new HashMap<>();
    process4Tags.put("status", "old");
    MockNodeImpl process4Node = new MockNodeImpl(3, "p4", process4Tags, host4Node);

    // Add children to hosts.
    host1Node.addChild(process1Node);
    host2Node.addChild(process2Node);
    host3Node.addChild(process3Node);
    host4Node.addChild(process4Node);

    // Generate an entity leaf node under each process -- i.e. 4 entities in total.
    //********************************************************************************************//
    Map<String, Object> entity1Tags = new HashMap<>();
    entity1Tags.put("leader", null);
    entity1Tags.put("answer", 1);
    MockLeafNodeImpl entity1Node = new MockLeafNodeImpl(4, "e1", entity1Tags, process1Node);

    Map<String, Object> entity2Tags = new HashMap<>();
    entity2Tags.put("leader", entity1Node);
    entity2Tags.put("answer", 2);
    MockLeafNodeImpl entity2Node = new MockLeafNodeImpl(4, "e2", entity2Tags, process2Node);

    Map<String, Object> entity3Tags = new HashMap<>();
    entity3Tags.put("leader", entity1Node);
    entity3Tags.put("answer", 3);
    MockLeafNodeImpl entity3Node = new MockLeafNodeImpl(4, "e3", entity3Tags, process3Node);

    Map<String, Object> entity4Tags = new HashMap<>();
    entity4Tags.put("leader", entity1Node);
    entity4Tags.put("answer", 4);
    MockLeafNodeImpl entity4Node = new MockLeafNodeImpl(4, "e4", entity4Tags, process4Node);

    // Add children to processes.
    process1Node.addChild(entity1Node);
    process2Node.addChild(entity2Node);
    process3Node.addChild(entity3Node);
    process4Node.addChild(entity4Node);

    return clusterNode;
  }

}
