/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol;

/**
 * Utils class for Cruise Control
 */
public class CruiseControlUtils {
  private CruiseControlUtils() {

  }

  public static void ensureValidString(String fieldName, String toCheck) {
    if (toCheck == null || toCheck.isEmpty()) {
      throw new IllegalArgumentException(fieldName + " cannot be null");
    }
  }


}
