/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.monitor.sampling.aggregator;

/**
 * There are a few extrapolations we will do when there is not
 * {@link com.linkedin.cruisecontrol.config.CruiseControlConfig#MIN_SAMPLES_PER_METRICS_WINDOW_CONFIG sufficient samples}
 * in a window for an entity. The extrapolations are used in the following preference order.
 * <ul>
 *   <li>
 *     NONE: No extrapolation was needed.
 *   </li>
 *   <li>
 *     AVG_AVAILABLE: The average of available samples. Used when there are less than required samples but more than
 *     half of the required samples.
 *   </li>
 *   <li>
 *     AVG_ADJACENT: The average value of the current window and the two adjacent windows. Used when there is
 *     less than half of the required samples and the two adjacent windows are valid.
 *   </li>
 *   <li>
 *     FORCED_INSUFFICIENT: The sample is forced to be included with insufficient data. Used when there is at least
 *     one sample in the window and none of the more favorable extrapolation works.
 *   </li>
 *   <li>
 *     NO_VALID_EXTRAPOLATION: there is no valid extrapolation. Used when none of the other extrapolation works.
 *   </li>
 * </ul>
 */
public enum Extrapolation {
  NONE, AVG_AVAILABLE, AVG_ADJACENT, FORCED_INSUFFICIENT, NO_VALID_EXTRAPOLATION
}
