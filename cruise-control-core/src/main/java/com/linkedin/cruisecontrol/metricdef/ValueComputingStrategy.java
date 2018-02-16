/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.metricdef;

/**
 * Define the way to pick the metric values among all the samples in a snapshot window.
 */
public enum ValueComputingStrategy {
  AVG, MAX, LATEST
}
