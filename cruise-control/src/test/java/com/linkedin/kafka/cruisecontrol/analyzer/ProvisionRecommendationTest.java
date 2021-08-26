/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import java.util.Collections;
import java.util.regex.Pattern;
import org.junit.Test;

import static org.junit.Assert.assertThrows;


public class ProvisionRecommendationTest {

  @Test
  public void testBuilderInput() {
    // Verify: state
    assertThrows(IllegalArgumentException.class, () -> new ProvisionRecommendation.Builder(ProvisionStatus.UNDECIDED));
    assertThrows(IllegalArgumentException.class, () -> new ProvisionRecommendation.Builder(ProvisionStatus.RIGHT_SIZED));

    // Verify: numBrokers
    assertThrows(IllegalArgumentException.class, () -> new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED).numBrokers(0));
    assertThrows(IllegalArgumentException.class, () -> new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED).numBrokers(-1));

    // Verify: numRacks
    assertThrows(IllegalArgumentException.class, () -> new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED).numRacks(0));
    assertThrows(IllegalArgumentException.class, () -> new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED).numRacks(-1));

    // Verify: numDisks
    assertThrows(IllegalArgumentException.class, () -> new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED).numDisks(0));
    assertThrows(IllegalArgumentException.class, () -> new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED).numDisks(-1));

    // Verify: numPartitions
    assertThrows(IllegalArgumentException.class, () -> new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED).numPartitions(0));
    assertThrows(IllegalArgumentException.class, () -> new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED).numPartitions(-1));

    // Verify: topic pattern
    assertThrows(IllegalArgumentException.class, () -> new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED).topicPattern(null));
    Pattern emptyPattern = Pattern.compile("");
    assertThrows(IllegalArgumentException.class, () -> new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED)
        .topicPattern(emptyPattern));

    // Verify: typicalBrokerId
    assertThrows(IllegalArgumentException.class, () -> new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED).typicalBrokerId(-1));

    // Verify: typicalBrokerCapacity
    assertThrows(IllegalArgumentException.class, () -> new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED)
        .typicalBrokerCapacity(0));
    assertThrows(IllegalArgumentException.class, () -> new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED)
        .typicalBrokerCapacity(-1));

    // Verify: excludedRackIds
    assertThrows(IllegalArgumentException.class, () -> new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED).excludedRackIds(null));
    assertThrows(IllegalArgumentException.class, () -> new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED)
        .excludedRackIds(Collections.emptySet()));
    assertThrows(IllegalArgumentException.class, () -> new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED)
        .excludedRackIds(Collections.singleton("")));

    // Verify: totalCapacity
    assertThrows(IllegalArgumentException.class, () -> new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED).totalCapacity(0));
    assertThrows(IllegalArgumentException.class, () -> new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED).totalCapacity(-1));
  }

  @Test
  public void testSanityCheckResources() {
    // Set multiple resources
    assertThrows(IllegalArgumentException.class, () -> ProvisionRecommendation.sanityCheckResources(
        new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED).numBrokers(1).numRacks(1)));

    // Set numPartitions under over-provisioned state
    assertThrows(IllegalArgumentException.class, () -> ProvisionRecommendation.sanityCheckResources(
        new ProvisionRecommendation.Builder(ProvisionStatus.OVER_PROVISIONED).numPartitions(1)));

    // Set numPartitions without the topic
    assertThrows(IllegalArgumentException.class, () -> ProvisionRecommendation.sanityCheckResources(
        new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED).numPartitions(1)));
  }

  @Test
  public void testSanityCheckTypical() {
    // Set a typical broker id without its capacity.
    assertThrows(IllegalArgumentException.class, () -> ProvisionRecommendation.sanityCheckTypical(
        new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED).numBrokers(1).typicalBrokerId(1).resource(Resource.CPU)));

    // Skip setting numBrokers.
    assertThrows(IllegalArgumentException.class, () -> ProvisionRecommendation.sanityCheckTypical(
        new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED).typicalBrokerId(1).typicalBrokerCapacity(1.0)
                                                                              .resource(Resource.CPU)));

    // Skip setting resource.
    assertThrows(IllegalArgumentException.class, () -> ProvisionRecommendation.sanityCheckTypical(
        new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED).typicalBrokerId(1).typicalBrokerCapacity(1.0).numBrokers(1)));
  }

  @Test
  public void testSanityCheckExcludedRackIds() {
    // Set excluded rack ids without numBrokers
    assertThrows(IllegalArgumentException.class, () -> ProvisionRecommendation.sanityCheckExcludedRackIds(
        new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED).numRacks(1).excludedRackIds(Collections.singleton("1"))));
  }
}
