/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;


public class ProvisionResponseTest {
  private static final String RECOMMENDER = ProvisionResponseTest.class.getSimpleName();
  private static final String UNDER_PROV_REC = "Add at least 6 brokers with the same capacity (3200.00) as broker-0.";
  private static final String OVER_PROV_REC = "Remove at least 4 brokers with the same capacity (1600.00) as broker-1.";

  private static ProvisionResponse generateProvisionResponse(ProvisionStatus status) {
    switch (status) {
      case UNDER_PROVISIONED:
        return new ProvisionResponse(ProvisionStatus.UNDER_PROVISIONED, UNDER_PROV_REC, RECOMMENDER);
      case RIGHT_SIZED:
        return new ProvisionResponse(ProvisionStatus.RIGHT_SIZED);
      case OVER_PROVISIONED:
        return new ProvisionResponse(ProvisionStatus.OVER_PROVISIONED, OVER_PROV_REC, RECOMMENDER);
      case UNDECIDED:
        return new ProvisionResponse(ProvisionStatus.UNDECIDED);
      default:
        throw new IllegalArgumentException("Unsupported provision status " + status + " is provided.");
    }
  }

  @Test
  public void testAggregate() {
    // Verify validity of input while creating a ProvisionResponse.
    assertThrows(IllegalArgumentException.class, () -> new ProvisionResponse(ProvisionStatus.RIGHT_SIZED, OVER_PROV_REC, RECOMMENDER));
    assertThrows(IllegalArgumentException.class, () -> new ProvisionResponse(ProvisionStatus.UNDECIDED, OVER_PROV_REC, RECOMMENDER));

    // Verify validity of aggregation (1) state and (2) recommendation.
    // Case-1: Aggregating any provision status with {@link ProvisionStatus#UNDER_PROVISIONED} is {@link ProvisionStatus#UNDER_PROVISIONED}.
    for (ProvisionStatus status : ProvisionStatus.cachedValues()) {
      ProvisionResponse underProvisioned = new ProvisionResponse(ProvisionStatus.UNDER_PROVISIONED, UNDER_PROV_REC, RECOMMENDER);
      underProvisioned.aggregate(generateProvisionResponse(status));
      assertEquals(ProvisionStatus.UNDER_PROVISIONED, underProvisioned.status());
      assertEquals(status == ProvisionStatus.UNDER_PROVISIONED
                   ? String.format("[%s] %s [%s] %s", RECOMMENDER, UNDER_PROV_REC, RECOMMENDER, UNDER_PROV_REC)
                   : String.format("[%s] %s", RECOMMENDER, UNDER_PROV_REC), underProvisioned.recommendation());
    }

    // Case-2: Aggregating a provision status {@code P} with {@link ProvisionStatus#UNDECIDED} is {@code P}
    for (ProvisionStatus status : ProvisionStatus.cachedValues()) {
      ProvisionResponse undecided = new ProvisionResponse(ProvisionStatus.UNDECIDED);
      ProvisionResponse other = generateProvisionResponse(status);
      String recommendationBefore = other.recommendation();
      undecided.aggregate(other);
      assertEquals(status, undecided.status());
      assertEquals(recommendationBefore, undecided.recommendation());
    }

    // Case-3.1: Aggregating {@link ProvisionStatus#RIGHT_SIZED} with {@link ProvisionStatus#RIGHT_SIZED} or
    // {@link ProvisionStatus#OVER_PROVISIONED} is {@link ProvisionStatus#RIGHT_SIZED}
    ProvisionResponse rightSized = new ProvisionResponse(ProvisionStatus.RIGHT_SIZED);
    rightSized.aggregate(generateProvisionResponse(ProvisionStatus.RIGHT_SIZED));
    assertEquals(ProvisionStatus.RIGHT_SIZED, rightSized.status());
    rightSized.aggregate(generateProvisionResponse(ProvisionStatus.OVER_PROVISIONED));
    assertEquals(ProvisionStatus.RIGHT_SIZED, rightSized.status());
    assertTrue(rightSized.recommendation().isEmpty());

    // Case-3.2: Aggregating {@link ProvisionStatus#OVER_PROVISIONED} with {@link ProvisionStatus#RIGHT_SIZED} clears the recommendation
    ProvisionResponse overProvisioned = new ProvisionResponse(ProvisionStatus.OVER_PROVISIONED, OVER_PROV_REC, RECOMMENDER);
    assertFalse(overProvisioned.recommendation().isEmpty());
    overProvisioned.aggregate(generateProvisionResponse(ProvisionStatus.RIGHT_SIZED));
    assertTrue(overProvisioned.recommendation().isEmpty());

    // Case-4: Aggregating {@link ProvisionStatus#OVER_PROVISIONED} with {@link ProvisionStatus#OVER_PROVISIONED} yields itself
    overProvisioned = new ProvisionResponse(ProvisionStatus.OVER_PROVISIONED, OVER_PROV_REC, RECOMMENDER);
    assertEquals(String.format("[%s] %s", RECOMMENDER, OVER_PROV_REC), overProvisioned.recommendation());
    overProvisioned.aggregate(generateProvisionResponse(ProvisionStatus.OVER_PROVISIONED));
    assertEquals(ProvisionStatus.OVER_PROVISIONED, overProvisioned.status());
    assertEquals(String.format("[%s] %s [%s] %s", RECOMMENDER, OVER_PROV_REC, RECOMMENDER, OVER_PROV_REC), overProvisioned.recommendation());
  }
}
