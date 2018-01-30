/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.linkedin.kafka.cruisecontrol.analyzer.BalancingProposal;
import com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer;
import com.linkedin.kafka.cruisecontrol.common.BalancingAction;
import javax.servlet.http.HttpServletRequest;


/**
 * The util class for Kafka Cruise Control servlet.
 */
class KafkaCruiseControlServletUtils {

  private KafkaCruiseControlServletUtils() {

  }

  private static final String[] HEADERS_TO_TRY = {
    "X-Forwarded-For",
    "Proxy-Client-IP",
    "WL-Proxy-Client-IP",
    "HTTP_X_FORWARDED_FOR",
    "HTTP_X_FORWARDED",
    "HTTP_X_CLUSTER_CLIENT_IP",
    "HTTP_CLIENT_IP",
    "HTTP_FORWARDED_FOR",
    "HTTP_FORWARDED",
    "HTTP_VIA",
    "REMOTE_ADDR"
  };

  static String getClientIpAddress(HttpServletRequest request) {
    for (String header : HEADERS_TO_TRY) {
      String ip = request.getHeader(header);
      if (ip != null && ip.length() != 0 && !"unknown".equalsIgnoreCase(ip)) {
        return ip;
      }
    }
    return request.getRemoteAddr();
  }

  static String getProposalSummary(GoalOptimizer.OptimizerResult result) {
    int numReplicaMovements = 0;
    int numLeaderMovements = 0;
    long dataToMove = 0;
    for (BalancingProposal p : result.goalProposals()) {
      if (p.balancingAction() == BalancingAction.REPLICA_MOVEMENT) {
        numReplicaMovements++;
        dataToMove += p.dataToMove();
      } else if (p.balancingAction() == BalancingAction.LEADERSHIP_MOVEMENT) {
        numLeaderMovements++;
      }
    }
    return String.format("%n%nThe optimization proposal has %d replica(%d MB) movements and %d leadership movements "
                             + "based on the cluster model with %d recent snapshot windows and %.3f%% of the partitions "
                             + "covered.", numReplicaMovements, dataToMove, numLeaderMovements,
                         result.clusterModelStats().numSnapshotWindows(),
                         result.clusterModelStats().monitoredPartitionsPercentage() * 100);
  }

}
