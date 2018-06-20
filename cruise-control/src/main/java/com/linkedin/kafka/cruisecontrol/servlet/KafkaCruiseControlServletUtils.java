/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import java.util.HashMap;
import java.util.Map;
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

  static String getProposalSummaryInString(GoalOptimizer.OptimizerResult result) {
    int numReplicaMovements = 0;
    int numLeaderMovements = 0;
    long dataToMove = 0;
    for (ExecutionProposal p : result.goalProposals()) {
      if (!p.replicasToAdd().isEmpty() || !p.replicasToRemove().isEmpty()) {
        numReplicaMovements++;
        dataToMove += p.dataToMoveInMB();
      } else {
        numLeaderMovements++;
      }
    }
    return String.format("%n%nThe optimization proposal has %d replica(%d MB) movements and %d leadership movements "
            + "based on the cluster model with %d recent snapshot windows and %.3f%% of the partitions " + "covered.",
            numReplicaMovements, dataToMove, numLeaderMovements, result.clusterModelStats().numSnapshotWindows(),
            result.clusterModelStats().monitoredPartitionsPercentage() * 100);
  }

  static Map<String, Object> getProposalSummaryInJson(GoalOptimizer.OptimizerResult result) {
    int numReplicaMovements = 0;
    int numLeaderMovements = 0;
    long dataToMove = 0;
    for (ExecutionProposal p : result.goalProposals()) {
      if (!p.replicasToAdd().isEmpty() || !p.replicasToRemove().isEmpty()) {
        numReplicaMovements++;
        dataToMove += p.dataToMoveInMB();
      } else {
        numLeaderMovements++;
      }
    }
    Map<String, Object> ret = new HashMap<>();
    ret.put("numReplicaMovements", numReplicaMovements);
    ret.put("dataToMoveMB", dataToMove);
    ret.put("numLeaderMovements", numLeaderMovements);
    ret.put("recentWindows", result.clusterModelStats().numSnapshotWindows());
    ret.put("monitoredPartitionsPercentage", result.clusterModelStats().monitoredPartitionsPercentage() * 100.0);
    return ret;
  }
}
