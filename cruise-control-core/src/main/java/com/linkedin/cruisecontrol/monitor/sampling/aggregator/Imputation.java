/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.monitor.sampling.aggregator;

/**
 * There are a few imputations we will do when there is not sufficient samples in a window for a
 * partition. The imputations are used in the following preference order.
 * <ul>
 *   <li>NONE: No imputation was needed.</li>
 *   <li>AVG_AVAILABLE: The average of available samples even though there are more than half of the required samples.</li>
 *   <li>AVG_ADJACENT: The average value of the current window and the two adjacent windows</li>
 *   <li>FORCED_INSUFFICIENT: The sample is forced to be included with insufficient data.</li>
 *   <li>NO_VALID_IMPUTATION: there is no valid imputation</li>
 * </ul>
 */
public enum Imputation {
  NONE, AVG_AVAILABLE, AVG_ADJACENT, FORCED_INSUFFICIENT, NO_VALID_IMPUTATION
}
