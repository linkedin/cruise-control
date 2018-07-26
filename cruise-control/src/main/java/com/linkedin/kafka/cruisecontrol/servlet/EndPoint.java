/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import java.util.Arrays;
import java.util.List;


/**
 * An enum that lists all supported endpoints by Cruise Control.
 */
enum EndPoint {
  BOOTSTRAP,
  TRAIN,
  LOAD,
  PARTITION_LOAD,
  PROPOSALS,
  STATE,
  ADD_BROKER,
  REMOVE_BROKER,
  REBALANCE,
  STOP_PROPOSAL_EXECUTION,
  PAUSE_SAMPLING,
  RESUME_SAMPLING,
  KAFKA_CLUSTER_STATE,
  DEMOTE_BROKER;

  private static final List<EndPoint> GET_ENDPOINT = Arrays.asList(BOOTSTRAP,
      TRAIN,
      LOAD,
      PARTITION_LOAD,
      PROPOSALS,
      STATE,
      KAFKA_CLUSTER_STATE);
  private static final List<EndPoint> POST_ENDPOINT = Arrays.asList(ADD_BROKER,
      REMOVE_BROKER,
      REBALANCE,
      STOP_PROPOSAL_EXECUTION,
      PAUSE_SAMPLING,
      RESUME_SAMPLING,
      DEMOTE_BROKER);

  public static List<EndPoint> getEndpoint() {
    return GET_ENDPOINT;
  }

  public static List<EndPoint> postEndpoint() {
    return POST_ENDPOINT;
  }
}
