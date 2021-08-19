/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.cruisecontrol;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class CruiseControlUtilsTest {
  private static final long MAX_ESTIMATED_PAUSE = 30L;
  private static final Map<Long, String> RESPONSE_BY_TIME_MS;
  static {
    RESPONSE_BY_TIME_MS =
        Map.of(0L, "1970-01-01T00:00:00Z", -10L, "1969-12-31T23:59:59Z", Long.MAX_VALUE, "+292278994-08-17T07:12:55Z", 1614978098383L,
               "2021-03-05T21:01:38Z");
  }

  @Test
  public void testUtcDate() {
    for (Map.Entry<Long, String> entry : RESPONSE_BY_TIME_MS.entrySet()) {
      assertEquals(entry.getValue(), CruiseControlUtils.utcDateFor(entry.getKey()));
    }

    Instant currentUtcDateFor = Instant.parse(CruiseControlUtils.utcDateFor(Instant.now().toEpochMilli()));
    Instant currentUtcDate = Instant.parse(CruiseControlUtils.currentUtcDate());
    long difference = currentUtcDate.minus(currentUtcDateFor.getEpochSecond(), ChronoUnit.SECONDS).getEpochSecond();

    // Maximum difference accounts for a potential GC pause -- ideally (1) currentUtcDateFor and (2) currentUtcDate should be the same.
    assertTrue(difference >= 0);
    assertTrue(difference < MAX_ESTIMATED_PAUSE);
  }
}
