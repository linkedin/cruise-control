/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionRecommendation;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


/**
 * A broker provisioner test based on using {@link BasicProvisioner} for broker provisioning.
 */
public class BasicBrokerProvisionerTest extends AbstractProvisionerTest {
  public static final ProvisionRecommendation BROKER_REC_TO_IGNORE = new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED)
      .numBrokers(1).resource(Resource.DISK).build();
  public static final String RECOMMENDER_TO_IGNORE = "ToIgnore";

  @Test
  public void testProvisionBrokerRecommendation() {
    ProvisionerState.State expectedState = ProvisionerState.State.COMPLETED;
    String expectedSummary = String.format("Provisioner support is missing. Skip recommendation: %s", BROKER_REC_TO_EXECUTE);

    Map<String, ProvisionRecommendation> provisionRecommendation = Map.of(RECOMMENDER_TO_IGNORE, BROKER_REC_TO_IGNORE,
                                                                          RECOMMENDER_TO_EXECUTE, BROKER_REC_TO_EXECUTE);

    ProvisionerState results = _provisioner.rightsize(provisionRecommendation, RIGHTSIZE_OPTIONS);

    assertEquals(expectedState, results.state());
    assertEquals(expectedSummary, results.summary());
  }
}
