/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.linkedin.cruisecontrol.servlet.EndPoint;
import com.linkedin.cruisecontrol.servlet.EndpointType;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndpointType.*;


public enum CruiseControlEndPoint implements EndPoint {
  BOOTSTRAP(CRUISE_CONTROL_ADMIN),
  TRAIN(CRUISE_CONTROL_ADMIN),
  LOAD(KAFKA_MONITOR),
  PARTITION_LOAD(KAFKA_MONITOR),
  PROPOSALS(KAFKA_MONITOR),
  STATE(CRUISE_CONTROL_MONITOR),
  ADD_BROKER(KAFKA_ADMIN),
  REMOVE_BROKER(KAFKA_ADMIN),
  FIX_OFFLINE_REPLICAS(KAFKA_ADMIN),
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

  private final EndpointType _endpointType;

  CruiseControlEndPoint(EndpointType endpointType) {
    _endpointType = endpointType;
  }

  @Override
  public EndpointType endpointType() {
    return _endpointType;
  }

  private static final List<CruiseControlEndPoint> GET_ENDPOINTS = Arrays.asList(BOOTSTRAP,
                                                                                 TRAIN,
                                                                                 LOAD,
                                                                                 PARTITION_LOAD,
                                                                                 PROPOSALS,
                                                                                 STATE,
                                                                                 KAFKA_CLUSTER_STATE,
                                                                                 USER_TASKS,
                                                                                 REVIEW_BOARD);
  private static final List<CruiseControlEndPoint> POST_ENDPOINTS = Arrays.asList(ADD_BROKER,
                                                                                  REMOVE_BROKER,
                                                                                  FIX_OFFLINE_REPLICAS,
                                                                                  REBALANCE,
                                                                                  STOP_PROPOSAL_EXECUTION,
                                                                                  PAUSE_SAMPLING,
                                                                                  RESUME_SAMPLING,
                                                                                  DEMOTE_BROKER,
                                                                                  ADMIN,
                                                                                  REVIEW,
                                                                                  TOPIC_CONFIGURATION);

  private static final List<CruiseControlEndPoint> CACHED_VALUES = Collections.unmodifiableList(Arrays.asList(values()));

  public static List<CruiseControlEndPoint> getEndpoints() {
    return GET_ENDPOINTS;
  }

  public static List<CruiseControlEndPoint> postEndpoints() {
    return POST_ENDPOINTS;
  }

  /**
   * @return Cached values of the enum.
   */
  public static List<CruiseControlEndPoint> cachedValues() {
    return CACHED_VALUES;
  }
}
