/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.fail;


/**
 * Unit test for making sure that loads in the deterministically generated cluster / rack / broker / replica are
 * consistent with each other.
 */
@RunWith(Parameterized.class)
public class LoadConsistencyTest {
  private final ClusterModel _clusterModel;
  private final boolean _shouldPassSanityCheck;

  /**
   * Constructor of LoadConsistencyTest.
   */
  public LoadConsistencyTest(ClusterModel clusterModel, boolean shouldPassSanityCheck) {
    _clusterModel = clusterModel;
    _shouldPassSanityCheck = shouldPassSanityCheck;
  }

  /**
   * Populate parameters for the parametrized test.
   * @return Populated parameters.
   */
  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    Collection<Object[]> params = new ArrayList<>();

    Map<Resource, Double> brokerCapacity = new HashMap<>();
    brokerCapacity.put(Resource.CPU, TestConstants.LARGE_BROKER_CAPACITY);
    brokerCapacity.put(Resource.DISK, TestConstants.LARGE_BROKER_CAPACITY);
    brokerCapacity.put(Resource.NW_IN, TestConstants.LARGE_BROKER_CAPACITY);
    brokerCapacity.put(Resource.NW_OUT, TestConstants.MEDIUM_BROKER_CAPACITY);

    // --SUCCESS: sum of load on sub-components equals the load of component.
    // Test for success after removing replica.
    ClusterModel smallClusterModel = DeterministicCluster.smallClusterModel(brokerCapacity);
    TopicPartition pInfoT10 = new TopicPartition("T1", 0);
    smallClusterModel.removeReplica(0, pInfoT10);
    Object[] smallClusterModelParams = {smallClusterModel, true};
    params.add(smallClusterModelParams);

    ClusterModel mediumClusterModel = DeterministicCluster.mediumClusterModel(brokerCapacity);
    TopicPartition pInfoB0 = new TopicPartition("B", 0);
    mediumClusterModel.removeReplica(1, pInfoB0);
    Object[] mediumClusterModelParams = {mediumClusterModel, true};
    params.add(mediumClusterModelParams);

    // Test for success after relocating replica.
    ClusterModel smallReplicaMoveClusterModel = DeterministicCluster.smallClusterModel(brokerCapacity);
    smallReplicaMoveClusterModel.relocateReplica(pInfoT10, 0, 1);
    Object[] smallReplicaMoveClusterModelParams = {smallReplicaMoveClusterModel, true};
    params.add(smallReplicaMoveClusterModelParams);

    ClusterModel mediumReplicaMoveClusterModel = DeterministicCluster.mediumClusterModel(brokerCapacity);
    mediumReplicaMoveClusterModel.relocateReplica(pInfoB0, 1, 0);
    Object[] mediumReplicaMoveClusterModelParams = {mediumReplicaMoveClusterModel, true};
    params.add(mediumReplicaMoveClusterModelParams);

    // --FAILURE:  sum of load on sub-components not equal to the load of component.
    // Test for failure after removing replica in low level of abstraction only.
    ClusterModel smallFaultyClusterModel = DeterministicCluster.smallClusterModel(brokerCapacity);
    smallFaultyClusterModel.broker(2).removeReplica(pInfoT10);
    Object[] smallFaultyClusterModelParams = {smallFaultyClusterModel, false};
    params.add(smallFaultyClusterModelParams);

    ClusterModel mediumFaultyClusterModel = DeterministicCluster.mediumClusterModel(brokerCapacity);
    mediumFaultyClusterModel.broker(1).removeReplica(pInfoB0);
    Object[] mediumFaultyClusterModelParams = {mediumFaultyClusterModel, false};
    params.add(mediumFaultyClusterModelParams);

    // Test for failure after adding replica in low level of abstraction only.
    ClusterModel smallFaultyReplicaAddClusterModel = DeterministicCluster.smallClusterModel(brokerCapacity);
    smallFaultyReplicaAddClusterModel.broker(0)
                                     .addReplica(new Replica(pInfoT10, smallFaultyReplicaAddClusterModel.broker(1), false));
    Object[] smallFaultyReplicaAddClusterModelParams = {smallFaultyReplicaAddClusterModel, false};
    params.add(smallFaultyReplicaAddClusterModelParams);

    ClusterModel mediumFaultyReplicaAddClusterModel = DeterministicCluster.mediumClusterModel(brokerCapacity);
    mediumFaultyReplicaAddClusterModel.broker(0)
                                      .addReplica(new Replica(pInfoB0, mediumFaultyReplicaAddClusterModel.broker(1), false));
    Object[] mediumFaultyReplicaAddClusterModelParams = {mediumFaultyReplicaAddClusterModel, false};
    params.add(mediumFaultyReplicaAddClusterModelParams);

    return params;
  }

  @Test
  public void test() {
    if (_shouldPassSanityCheck) {
      _clusterModel.sanityCheck();
    } else {
      try {
        _clusterModel.sanityCheck();
        fail("Should throw IllegalArgumentException");
      } catch (IllegalArgumentException mie) {
        // Let it go.
      }
    }
  }
}
