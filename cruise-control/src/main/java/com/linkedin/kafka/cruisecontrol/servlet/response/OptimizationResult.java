/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.servlet.EndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.AddedOrRemovedBrokerParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.DemoteBrokerParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.KafkaOptimizationParameters;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.JSON_VERSION;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.VERSION;


public class OptimizationResult extends AbstractCruiseControlResponse {
  private static final Logger LOG = LoggerFactory.getLogger(OptimizationResult.class);
  private final GoalOptimizer.OptimizerResult _optimizerResult;
  private static final String SUMMARY = "summary";
  private static final String PROPOSALS = "proposals";
  private static final String GOAL = "goal";
  private static final String GOAL_SUMMARY = "goalSummary";
  private static final String STATUS = "status";
  private static final String CLUSTER_MODEL_STATS = "clusterModelStats";
  private static final String LOAD_AFTER_OPTIMIZATION = "loadAfterOptimization";
  private static final String LOAD_BEFORE_OPTIMIZATION = "loadBeforeOptimization";
  private static final String VIOLATED = "VIOLATED";
  private static final String FIXED = "FIXED";
  private static final String NO_ACTION = "NO-ACTION";

  public OptimizationResult(GoalOptimizer.OptimizerResult optimizerResult) {
    _optimizerResult = optimizerResult;
  }

  public GoalOptimizer.OptimizerResult optimizerResult() {
    return _optimizerResult;
  }

  @Override
  public void writeOutputStream(OutputStream out, CruiseControlParameters parameters) {
    try {
      EndPoint endPoint = parameters.endPoint();
      String pretext;
      switch (endPoint) {
        case ADD_BROKER:
          pretext = String.format("%n%nCluster load after adding broker %s:%n", ((AddedOrRemovedBrokerParameters) parameters).brokerIds());
          break;
        case REMOVE_BROKER:
          pretext = String.format("%n%nCluster load after removing broker %s:%n", ((AddedOrRemovedBrokerParameters) parameters).brokerIds());
          break;
        case FIX_OFFLINE_REPLICAS:
          pretext = String.format("%n%nCluster load after fixing offline replicas:%n");
          break;
        case PROPOSALS:
          pretext = String.format("%n%nOptimized load:%n");
          break;
        case REBALANCE:
          pretext = String.format("%n%nCluster load after rebalance:%n");
          break;
        case DEMOTE_BROKER:
          pretext = String.format("%n%nCluster load after demoting broker %s:%n", ((DemoteBrokerParameters) parameters).brokerIds());
          break;
        default:
          LOG.error("Unrecognized endpoint.");
          return;
      }

      boolean isVerbose = ((KafkaOptimizationParameters) parameters).isVerbose();
      if (isVerbose) {
        out.write(_optimizerResult.goalProposals().toString().getBytes(StandardCharsets.UTF_8));
      }

      writeProposalSummary(out);
      if (isVerbose) {
        out.write(String.format("%n%nCurrent load:%n%s", _optimizerResult.brokerStatsBeforeOptimization().toString())
                        .getBytes(StandardCharsets.UTF_8));
      }

      out.write(pretext.getBytes(StandardCharsets.UTF_8));
      out.write(_optimizerResult.brokerStatsAfterOptimization().toString().getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      LOG.error("Failed to write output stream.", e);
    }
  }

  @Override
  public String getJSONString(CruiseControlParameters parameters) {
    Map<String, Object> optimizationResult = new HashMap<>();
    if (((KafkaOptimizationParameters) parameters).isVerbose()) {
      optimizationResult.put(PROPOSALS, _optimizerResult.goalProposals().stream()
                                                        .map(ExecutionProposal::getJsonStructure).collect(Collectors.toSet()));
      optimizationResult.put(LOAD_BEFORE_OPTIMIZATION, _optimizerResult.brokerStatsBeforeOptimization().getJsonStructure());
    }

    optimizationResult.put(SUMMARY, _optimizerResult.getProposalSummaryForJson());
    List<Map<String, Object>> goalSummary = new ArrayList<>();
    for (Map.Entry<Goal, ClusterModelStats> entry : _optimizerResult.statsByGoalPriority().entrySet()) {
      Goal goal = entry.getKey();
      Map<String, Object> goalMap = new HashMap<>();
      goalMap.put(GOAL, goal.name());
      goalMap.put(STATUS, goalResultDescription(goal));
      goalMap.put(CLUSTER_MODEL_STATS, entry.getValue().getJsonStructure());
      goalSummary.add(goalMap);
    }
    optimizationResult.put(GOAL_SUMMARY, goalSummary);
    optimizationResult.put(LOAD_AFTER_OPTIMIZATION, _optimizerResult.brokerStatsAfterOptimization().getJsonStructure());
    optimizationResult.put(VERSION, JSON_VERSION);
    Gson gson = new GsonBuilder().serializeNulls().serializeSpecialFloatingPointValues().create();

    return gson.toJson(optimizationResult);
  }

  private void writeProposalSummary(OutputStream out) throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append(_optimizerResult.getProposalSummary());
    for (Map.Entry<Goal, ClusterModelStats> entry : _optimizerResult.statsByGoalPriority().entrySet()) {
      Goal goal = entry.getKey();
      sb.append(String.format("%n%nStats for goal %s(%s):%n", goal.name(), goalResultDescription(goal)));
      sb.append(entry.getValue().toString());
    }
    out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
  }

  private String goalResultDescription(Goal goal) {
    return _optimizerResult.violatedGoalsBeforeOptimization().contains(goal) ?
           _optimizerResult.violatedGoalsAfterOptimization().contains(goal) ? VIOLATED : FIXED : NO_ACTION;
  }
}
