/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol;

import java.text.SimpleDateFormat;
import java.util.Date;


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
    if (toCheck == null || toCheck.isEmpty()) {
      throw new IllegalArgumentException(fieldName + " cannot be null");
    }
  }

  /**
   * Format the time to a pretty string.
   * @param time the time to format.
   * @return a pretty string for the time.
   */
  public static String toPrettyTime(long time) {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss");
    return sdf.format(new Date(time));
  }


}
