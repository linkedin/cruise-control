/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;


/**
 * The interface for a Kafka anomaly.
 */
abstract class KafkaAnomaly implements Anomaly<KafkaCruiseControl, KafkaCruiseControlException> {
}
