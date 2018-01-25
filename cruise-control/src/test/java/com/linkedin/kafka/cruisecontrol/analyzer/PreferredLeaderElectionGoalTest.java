/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.CruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.PreferredLeaderElectionGoal;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.exception.ModelInputException;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Load;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class PreferredLeaderElectionGoalTest {
  private static final String TOPIC0 = "topic0";
  private static final String TOPIC1 = "topic1";
  private static final String TOPIC2 = "topic2";

  private static final TopicPartition T0P0 = new TopicPartition(TOPIC0, 0);
  private static final TopicPartition T0P1 = new TopicPartition(TOPIC0, 1);
  private static final TopicPartition T0P2 = new TopicPartition(TOPIC0, 2);

  private static final TopicPartition T1P0 = new TopicPartition(TOPIC1, 0);
  private static final TopicPartition T1P1 = new TopicPartition(TOPIC1, 1);
  private static final TopicPartition T1P2 = new TopicPartition(TOPIC1, 2);

  private static final TopicPartition T2P0 = new TopicPartition(TOPIC2, 0);
  private static final TopicPartition T2P1 = new TopicPartition(TOPIC2, 1);
  private static final TopicPartition T2P2 = new TopicPartition(TOPIC2, 2);

  @Test
  public void testOptimize() throws KafkaCruiseControlException {
    ClusterModel clusterModel = createClusterModel();

    PreferredLeaderElectionGoal goal = new PreferredLeaderElectionGoal();
    goal.optimize(clusterModel, Collections.emptySet(), Collections.emptySet());

    for (String t : Arrays.asList(TOPIC0, TOPIC1, TOPIC2)) {
      for (int p = 0; p < 3; p++) {
        List<Replica> replicas = clusterModel.partition(new TopicPartition(t, p)).replicas();
        for (int i = 0; i < 3; i++) {
          // only the first replica should be leader.
          assertEquals(i == 0, replicas.get(i).isLeader());
        }
      }
    }
  }

  private ClusterModel createClusterModel() throws ModelInputException {
    int numSnapshots = 2;
    if (!Load.initialized()) {
      Properties props = CruiseControlUnitTestUtils.getCruiseControlProperties();
      props.setProperty(KafkaCruiseControlConfig.NUM_LOAD_SNAPSHOTS_CONFIG, Integer.toString(numSnapshots));
      Load.init(new KafkaCruiseControlConfig(props));
    }

    final int numRacks = 4;
    ClusterModel clusterModel = new ClusterModel(new ModelGeneration(0, 0),
                                                 1.0);
    for (int i = 0; i < numRacks; i++) {
      clusterModel.createRack("r" + i);
    }

    int i = 0;
    for (; i < 2; i++) {
      clusterModel.createBroker("r0", "h" + i, i, TestConstants.BROKER_CAPACITY);
    }
    for (int j = 1; j < numRacks; j++, i++) {
      clusterModel.createBroker("r" + j, "h" + i, i, TestConstants.BROKER_CAPACITY);
    }

    clusterModel.createReplica("r0", 0, T0P0, 0, true);
    clusterModel.createReplica("r0", 1, T0P1, 0, true);
    clusterModel.createReplica("r1", 2, T0P2, 0, true);
    clusterModel.createReplica("r2", 3, T1P0, 0, false);
    clusterModel.createReplica("r3", 4, T1P1, 0, false);
    clusterModel.createReplica("r0", 0, T1P2, 0, false);
    clusterModel.createReplica("r0", 1, T2P0, 0, false);
    clusterModel.createReplica("r1", 2, T2P1, 0, false);
    clusterModel.createReplica("r2", 3, T2P2, 0, false);

    clusterModel.createReplica("r3", 4, T0P0, 1, false);
    clusterModel.createReplica("r1", 2, T0P1, 1, false);
    clusterModel.createReplica("r0", 0, T0P2, 1, false);
    clusterModel.createReplica("r0", 1, T1P0, 1, true);
    clusterModel.createReplica("r2", 3, T1P1, 1, true);
    clusterModel.createReplica("r3", 4, T1P2, 1, true);
    clusterModel.createReplica("r1", 2, T2P0, 1, false);
    clusterModel.createReplica("r0", 0, T2P1, 1, false);
    clusterModel.createReplica("r0", 1, T2P2, 1, false);

    clusterModel.createReplica("r2", 3, T0P0, 2, false);
    clusterModel.createReplica("r3", 4, T0P1, 2, false);
    clusterModel.createReplica("r2", 3, T0P2, 2, false);
    clusterModel.createReplica("r1", 2, T1P0, 2, false);
    clusterModel.createReplica("r0", 0, T1P1, 2, false);
    clusterModel.createReplica("r1", 2, T1P2, 2, false);
    clusterModel.createReplica("r3", 4, T2P0, 2, true);
    clusterModel.createReplica("r2", 3, T2P1, 2, true);
    clusterModel.createReplica("r3", 4, T2P2, 2, true);

    return clusterModel;
  }

}
