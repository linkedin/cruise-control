/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.kafka.cruisecontrol.common.RandomCluster;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ClusterModelTest {

  @Test
  public void testClusterModelSerialization() throws Exception {
    ClusterModel clusterModel = RandomCluster.generate(TestConstants.BASE_PROPERTIES);
    RandomCluster.populate(clusterModel, TestConstants.BASE_PROPERTIES, TestConstants.Distribution.EXPONENTIAL);

    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(bout);
    oos.writeObject(clusterModel);
    oos.flush();

    ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
    ObjectInputStream oin = new ObjectInputStream(bin);
    ClusterModel deserializedClusterModel = (ClusterModel) oin.readObject();

    //ClusterModel.equals() turns out to be difficult since cluster model has
    //lots of circular references.  So these are just sanity checks.
    assertEquals(clusterModel.brokers().size(), deserializedClusterModel.brokers().size());
  }

}
