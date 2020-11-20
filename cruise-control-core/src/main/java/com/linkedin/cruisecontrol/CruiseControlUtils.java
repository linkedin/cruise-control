/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol;

import java.text.SimpleDateFormat;
import java.util.Date;
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
   * Format the time to a pretty string.
   * @param time the time to format.
   * @return A pretty string for the time.
   */
  public static String toPrettyTime(long time) {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss");
    return sdf.format(new Date(time));
  }


}
