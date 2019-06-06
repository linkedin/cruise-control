/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.linkedin.kafka.cruisecontrol.servlet.RetentionType.*;

/**
 * An enum for categorizing {@link EndPoint} w.r.t retention. Endpoints can be roughly grouped into following four types.
 *<ul>
 *  <li>KAFKA_MONITOR: endpoints that get information related to Kafka cluster.</li>
 *  <li>CRUISE_CONTROL_MONITOR: endpoints that get information related to Cruise Control instance.</li>
 *  <li>KAFKA_ADMIN: endpoints that can change state of Kafka cluster.</li>
 *  <li>CRUISE_CONTROL_ADMIN: endpoints that can change state or setting of Cruise Control instance.</li>
 *</ul>
 */
enum RetentionType {
  KAFKA_MONITOR,
  CRUISE_CONTROL_MONITOR,
  KAFKA_ADMIN,
  CRUISE_CONTROL_ADMIN;

  private static final List<RetentionType> CACHED_VALUES = Collections.unmodifiableList(Arrays.asList(values()));

  public static List<RetentionType> cachedValues() {
    return CACHED_VALUES;
  }
}

/**
 * An enum that lists all supported endpoints by Cruise Control.
 */
public enum EndPoint {
  BOOTSTRAP(CRUISE_CONTROL_ADMIN),
  TRAIN(CRUISE_CONTROL_ADMIN),
  LOAD(KAFKA_MONITOR),
  PARTITION_LOAD(KAFKA_MONITOR),
  PROPOSALS(KAFKA_MONITOR),
  STATE(CRUISE_CONTROL_MONITOR),
  ADD_BROKER(KAFKA_ADMIN),
  REMOVE_BROKER(KAFKA_ADMIN),
  REBALANCE(KAFKA_ADMIN),
  STOP_PROPOSAL_EXECUTION(KAFKA_ADMIN),
  PAUSE_SAMPLING(CRUISE_CONTROL_ADMIN),
  RESUME_SAMPLING(CRUISE_CONTROL_ADMIN),
  KAFKA_CLUSTER_STATE(KAFKA_MONITOR),
  DEMOTE_BROKER(KAFKA_ADMIN),
  USER_TASKS(CRUISE_CONTROL_MONITOR),
  REVIEW_BOARD(CRUISE_CONTROL_MONITOR),
  ADMIN(CRUISE_CONTROL_ADMIN),
  REVIEW(CRUISE_CONTROL_ADMIN),
  TOPIC_CONFIGURATION(KAFKA_ADMIN);

  private final RetentionType _retentionType;

  EndPoint(RetentionType retentionType) {
    _retentionType = retentionType;
  }

  public RetentionType retentionType() {
    return _retentionType;
  }

  private static final List<EndPoint> GET_ENDPOINT = Arrays.asList(BOOTSTRAP,
                                                                   TRAIN,
                                                                   LOAD,
                                                                   PARTITION_LOAD,
                                                                   PROPOSALS,
                                                                   STATE,
                                                                   KAFKA_CLUSTER_STATE,
                                                                   USER_TASKS,
                                                                   REVIEW_BOARD);
  private static final List<EndPoint> POST_ENDPOINT = Arrays.asList(ADD_BROKER,
                                                                    REMOVE_BROKER,
                                                                    REBALANCE,
                                                                    STOP_PROPOSAL_EXECUTION,
                                                                    PAUSE_SAMPLING,
                                                                    RESUME_SAMPLING,
                                                                    DEMOTE_BROKER,
                                                                    ADMIN,
                                                                    REVIEW,
                                                                    TOPIC_CONFIGURATION);

  private static final List<EndPoint> CACHED_VALUES = Collections.unmodifiableList(Arrays.asList(values()));

  public static List<EndPoint> getEndpoint() {
    return GET_ENDPOINT;
  }

  public static List<EndPoint> postEndpoint() {
    return POST_ENDPOINT;
  }

  /**
   * Use this instead of values() because values() creates a new array each time.
   * @return enumerated values in the same order as values()
   */
  public static List<EndPoint> cachedValues() {
    return CACHED_VALUES;
  }
}
