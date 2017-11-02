/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;


/**
 * The interface for an anomaly.
 */
abstract class Anomaly {

  /**
   * Fix the anomaly with KafkaCruiseControl. We make this method abstract so the user provided
   * @param kafkaCruiseControl the Kafka Cruise Control instance.
   */
  abstract void fix(KafkaCruiseControl kafkaCruiseControl) throws KafkaCruiseControlException;
}
