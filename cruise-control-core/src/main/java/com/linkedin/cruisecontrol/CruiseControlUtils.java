/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;

/**
 * Utils class for Cruise Control
 */
public class CruiseControlUtils {
  private CruiseControlUtils() {

  }

  /**
   * Ensure the string value of the string key is not null or empty.
   */
  public static void ensureValidString(String fieldName, String toCheck) {
    validateNotNull(toCheck, () -> fieldName + " cannot be null");
    if (toCheck.isEmpty()) {
      throw new IllegalArgumentException(fieldName + " cannot be empty");
    }
  }

  /**
   * @return The current UTC date.
   */
  public static String currentUtcDate() {
    return utcDateFor(Instant.now().getEpochSecond() * 1000);
  }

  /**
   * @see <a href="https://xkcd.com/1179/">https://xkcd.com/1179/</a>
   * @param timeMs Time in milliseconds.
   * @return The date for the given time in ISO 8601 format with date, hour, minute, and seconds.
   */
  public static String utcDateFor(long timeMs) {
    return utcDateFor(timeMs, 0, ChronoUnit.SECONDS);
  }
  
  /**
   * @see <a href="https://xkcd.com/1179/">https://xkcd.com/1179/</a>
   * @param timeMs Time in milliseconds.
   * @param precision requested time precision used in {@link DateTimeFormatterBuilder#appendInstant()}
   *        i.e: 0 for seconds precision, 3 for milliseconds, 6 for microseconds etc...
   * @param roundTo round the provided time to the provided {@link TemporalUnit}
   * @return The date for the given time in ISO 8601 format with provided precision (not truncated even if 0)
   */
  public static String utcDateFor(long timeMs, int precision, TemporalUnit roundTo) {
    DateTimeFormatter formatter = new DateTimeFormatterBuilder().appendInstant(precision).toFormatter();
    return formatter.format(Instant.ofEpochMilli(timeMs).truncatedTo(roundTo));
  }

}
