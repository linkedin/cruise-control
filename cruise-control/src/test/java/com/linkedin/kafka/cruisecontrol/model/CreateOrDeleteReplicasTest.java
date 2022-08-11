/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;

public class CreateOrDeleteReplicasTest {
  @Test
  public void testCreateOrDeleteReplicasSkippedOnModelInconsistency() {
    ClusterModel clusterModel = DeterministicCluster.smallClusterModel(TestConstants.BROKER_CAPACITY);
    Cluster clusterFromClusterModel = DeterministicCluster.generateClusterFromClusterModel(clusterModel);

    Map<Short, Set<String>> topicsByRF = new HashMap<>();
    topicsByRF.put((short) 1, Set.of(DeterministicCluster.T1));
    clusterModel.createOrDeleteReplicas(topicsByRF, Collections.emptyMap(), Collections.emptyMap(), clusterFromClusterModel);

    // Verify the delete replica works when the cluster is consistent with cluster model
    Assert.assertEquals(1, clusterModel.partition(new TopicPartition(DeterministicCluster.T1, 0)).replicas().size());

    Cluster updatedCluster = DeterministicCluster.generateClusterFromClusterModel(clusterModel);
    clusterModel = DeterministicCluster.smallClusterModel(TestConstants.BROKER_CAPACITY);
    clusterModel.createOrDeleteReplicas(topicsByRF, Collections.emptyMap(), Collections.emptyMap(),
                                        updatedCluster);
    // Verify the delete replica is skipped when the cluster is inconsistent with cluster model
    Assert.assertEquals(2, clusterModel.partition(new TopicPartition(DeterministicCluster.T1, 0)).replicas().size());
  }
}
