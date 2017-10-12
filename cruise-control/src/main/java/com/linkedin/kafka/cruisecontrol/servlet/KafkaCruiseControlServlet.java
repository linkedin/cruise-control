/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlState;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Partition;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.MetricSampleAggregationResult;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The servlet for kafka cruise control.
 */
public class KafkaCruiseControlServlet extends HttpServlet {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaCruiseControlServlet.class);
  private static final Logger ACCESS_LOG = LoggerFactory.getLogger("CruiseControlPublicAccessLogger");

  private static final String START_MS_PARAM = "start";
  private static final String END_MS_PARAM = "end";
  private static final String CLEAR_METRICS_PARAM = "clearmetrics";
  private static final String TIME_PARAM = "time";
  private static final String VERBOSE_PARAM = "verbose";
  private static final String SUPER_VERBOSE_PARM = "super_verbose";
  private static final String RESOURCE_PARAM = "resource";
  private static final String WITH_AVAILABLE_VALID_WINDOWS = "with_available_valid_windows";
  private static final String WITH_AVAILABLE_VALID_PARTITIONS = "with_available_valid_partitions";

  private static final String GOALS_PARAM = "goals";
  private static final String GRANULARITY_PARAM = "granularity";
  private static final String GRANULARITY_BROKER = "broker";

  private static final String GRANULARITY_REPLICA = "replica";
  private static final String BROKER_ID_PARAM = "brokerid";
  private static final String DRY_RUN_PARAM = "dryrun";
  private static final String THROTTLE_REMOVED_BROKER_PARAM = "throttle_removed_broker";
  private static final String IGNORE_PROPOSAL_CACHE_PARAM = "ignore_proposal_cache";

  private enum EndPoint {
    BOOTSTRAP,
    TRAIN,
    LOAD,
    PARTITION_LOAD,
    PROPOSALS,
    STATE,
    ADD_BROKER,
    REMOVE_BROKER,
    REBALANCE,
    STOP_PROPOSAL_EXECUTION
  }

  private KafkaCruiseControl _kafkaCruiseControl;

  public KafkaCruiseControlServlet(KafkaCruiseControl kafkaCruiseControl) {
    _kafkaCruiseControl = kafkaCruiseControl;
  }

  /**
   * The GET requests can do the following:
   *
   * <pre>
   * 1. Bootstrap the load monitor
   *    RANGE MODE:
   *      GET /kafkacruisecontrol/bootstrap?start=[START_TIMESTAMP]&end=[END_TIMESTAMP]
   *    SINCE MODE:
   *      GET /kafkacruisecontrol/bootstrap?start=[START_TIMESTAMP]
   *    RECENT MODE:
   *      GET /kafkacruisecontrol/bootstrap
   *
   * 2. Train the Kafka Cruise Control linear regression model. The trained model will only be used if
   *    use.linear.regression.model is set to true.
   *    GET /kafkacruisecontrol/train?start=[START_TIMESTAMP]&end=[END_TIMESTAMP]
   *
   * 3. Get the cluster load
   *    GET /kafkacruisecontrol/load?time=[TIMESTAMP]&granularity=[GRANULARITY]
   *    The valid granularity value are "replica" and "broker". The default is broker level.
   *
   * 4. Get the partition load sorted by the utilization of a given resource
   *    GET /kafkacruisecontrol/partition_load?resource=[RESOURCE]&start=[START_TIMESTAMP]&end=[END_TIMESTAMP]
   *
   * 5. Get an optimization proposal
   *    GET /kafkacruisecontrol/proposals?verbose=[ENABLE_VERBOSE]&ignore_proposal_cache=[true/false]
   *    &goals=[goal1,goal2...]&with_available_monitored_partitions=[true/false]&with_available_valid_windows=[true/false]
   *
   * 6. query the state of Kafka cruise control
   *    GET /kafkacruisecontrol/state
   *
   * <b>NOTE: All the timestamps are epoch time in second granularity.</b>
   * </pre>
   */
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    ACCESS_LOG.info("Received {}, {} from {}", urlEncode(request.toString()), urlEncode(request.getRequestURL().toString()),
                     KafkaCruiseControlServletUtils.getClientIpAddress(request));
    try {
      EndPoint endPoint = endPoint(request);
      if (endPoint != null) {
        switch (endPoint) {
          case BOOTSTRAP:
            bootstrap(request, response);
            break;
          case TRAIN:
            train(request, response);
            break;
          case LOAD:
            getClusterLoad(request, response);
            break;
          case PARTITION_LOAD:
            getPartitionLoad(request, response);
            break;
          case PROPOSALS:
            getProposals(request, response);
            break;
          case STATE:
            getState(request, response);
            break;
          default:
            throw new UserRequestException("Invalid URL for GET");
        }
      } else {
        String errorMessage = String.format("Bad GET request '%s'", request.getPathInfo());
        returnErrorMessage(response, errorMessage, HttpServletResponse.SC_NOT_FOUND);
      }
    } catch (UserRequestException ure) {
      LOG.error("Why are you failing?", ure);
      String errorMessage = String.format("Bad GET request '%s'", request.getPathInfo());
      returnErrorMessage(response, errorMessage, HttpServletResponse.SC_BAD_REQUEST);
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);
      String errorMessage = String.format("Error processing GET request '%s'%n%s", request.getPathInfo(), sw.toString());
      LOG.error(errorMessage);
      returnErrorMessage(response, errorMessage, HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    } finally {
      try {
        response.getOutputStream().close();
      } catch (IOException e) {
        LOG.warn("Error closing output stream: ", e);
      }
    }
  }

  /**
   * The POST method allows user to perform the following actions:
   *
   * <pre>
   * 1. Decommission a broker.
   *    POST /kafkacruisecontrol/remove_broker?brokerid=[id1,id2...]&dryrun=[true/false]&throttle_removed_broker=[true/false]&goals=[goal1,goal2...]
   *
   * 2. Add a broker
   *    POST /kafkacruisecontrol/add_broker?brokerid=[id1,id2...]&dryrun=[true/false]&throttle_removed_broker=[true/false]&goals=[goal1,goal2...]
   *
   * 3. Trigger a workload balance.
   *    POST /kafkacruisecontrol/rebalance?dryrun=[true/false]&force=[true/false]&goals=[goal1,goal2...]
   *
   * 4. Stop the proposal execution.
   *    POST /kafkacruisecontrol/stop_proposal_execution
   *
   *
   * <b>NOTE: All the timestamps are epoch time in second granularity.</b>
   * </pre>
   */
  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    ACCESS_LOG.info("Received {}, {} from {}", urlEncode(request.toString()), urlEncode(request.getRequestURL().toString()),
                    KafkaCruiseControlServletUtils.getClientIpAddress(request));
    try {
      EndPoint endPoint = endPoint(request);
      if (endPoint != null) {
        switch (endPoint) {
          case ADD_BROKER:
          case REMOVE_BROKER:
            addOrRemoveBroker(request, response, endPoint);
            break;
          case REBALANCE:
            rebalance(request, response);
            break;
          case STOP_PROPOSAL_EXECUTION:
            stopProposalExecution();
            break;
          default:
            throw new UserRequestException("Invalid url for POST");
        }
      } else {
        String errorMessage = String.format("Bad POST request '%s'", request.getPathInfo());
        returnErrorMessage(response, errorMessage, HttpServletResponse.SC_NOT_FOUND);
      }
    } catch (UserRequestException ure) {
      LOG.error("Why are you failing?", ure);
      String errorMessage = String.format("Bad POST request '%s'", request.getPathInfo());
      returnErrorMessage(response, errorMessage, HttpServletResponse.SC_BAD_REQUEST);
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);
      String errorMessage = String.format("Error processing POST request '%s'%n%s", request.getPathInfo(), sw.toString());
      LOG.error(errorMessage);
      returnErrorMessage(response, errorMessage, HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    } finally {
      try {
        response.getOutputStream().close();
      } catch (IOException e) {
        LOG.warn("Error closing output stream: ", e);
      }
    }
  }

  private void bootstrap(HttpServletRequest request, HttpServletResponse response) throws Exception {
    Long startMs;
    Long endMs;
    boolean clearMetrics;
    try {
      String startMsString = request.getParameter(START_MS_PARAM);
      String endMsString = request.getParameter(END_MS_PARAM);
      String clearMetricsString = request.getParameter(CLEAR_METRICS_PARAM);
      startMs = startMsString == null ? null : Long.parseLong(startMsString);
      endMs = endMsString == null ? null : Long.parseLong(endMsString);
      clearMetrics = clearMetricsString == null || Boolean.parseBoolean(clearMetricsString);
      if (startMs == null && endMs != null) {
        throw new IllegalArgumentException("The start time cannot be empty when end time is specified.");
      }
    } catch (Exception e) {
      throw new UserRequestException(e);
    }

    if (startMs != null && endMs != null) {
      _kafkaCruiseControl.bootstrapLoadMonitor(startMs, endMs, clearMetrics);
    } else if (startMs != null && endMs == null) {
      _kafkaCruiseControl.bootstrapLoadMonitor(startMs, clearMetrics);
    } else {
      _kafkaCruiseControl.bootstrapLoadMonitor(clearMetrics);
    }
    String resp = "Bootstrap started. Check status through " + getStateCheckUrl(request);
    setResponseCode(response, HttpServletResponse.SC_OK);
    response.setContentLength(resp.length());
    response.getOutputStream().write(resp.getBytes(StandardCharsets.UTF_8));
    response.getOutputStream().flush();
  }

  private void train(HttpServletRequest request, HttpServletResponse response) throws Exception {
    Long startMs;
    Long endMs;
    try {
      String startMsString = request.getParameter(START_MS_PARAM);
      String endMsString = request.getParameter(END_MS_PARAM);
      startMs = Long.parseLong(startMsString);
      endMs = Long.parseLong(endMsString);
    } catch (Exception e) {
      throw new UserRequestException(e);
    }
    _kafkaCruiseControl.trainLoadModel(startMs, endMs);
    String resp = "Load model training started. Check status through " + getStateCheckUrl(request);
    setResponseCode(response, HttpServletResponse.SC_OK);
    response.setContentLength(resp.length());
    response.getOutputStream().write(resp.getBytes(StandardCharsets.UTF_8));
    response.getOutputStream().flush();
  }

  private void getClusterLoad(HttpServletRequest request, HttpServletResponse response) throws Exception {
    long time;
    String granularity;
    try {
      String timeString = request.getParameter(TIME_PARAM);
      time = (timeString == null || timeString.toUpperCase().equals("NOW"))
          ? System.currentTimeMillis() : Long.parseLong(timeString);
      granularity = request.getParameter(GRANULARITY_PARAM);
    } catch (Exception e) {
      throw new UserRequestException(e);
    }

    ModelCompletenessRequirements requirements = new ModelCompletenessRequirements(1, 0.0, false);
    if (granularity == null || granularity.toLowerCase().equals(GRANULARITY_BROKER)) {
      ClusterModel.BrokerStats brokerStats = _kafkaCruiseControl.cachedBrokerLoadStats();
      String brokerLoad;
      if (brokerStats != null) {
        brokerLoad = brokerStats.toString();
      } else {
        ClusterModel clusterModel = _kafkaCruiseControl.clusterModel(time, requirements);
        brokerLoad = clusterModel.brokerStats().toString();
      }
      setResponseCode(response, HttpServletResponse.SC_OK);
      response.setContentLength(brokerLoad.length());
      response.getOutputStream().write(brokerLoad.getBytes(StandardCharsets.UTF_8));
    } else if (granularity.toLowerCase().equals(GRANULARITY_REPLICA)) {
      ClusterModel clusterModel = _kafkaCruiseControl.clusterModel(time, requirements);
      setResponseCode(response, HttpServletResponse.SC_OK);
      // Write to stream to avoid expensive toString() call.
      clusterModel.writeTo(response.getOutputStream());
    } else {
      throw new UserRequestException("Unknown granularity " + granularity);
    }
    response.getOutputStream().flush();
  }

  private void getPartitionLoad(HttpServletRequest request, HttpServletResponse response) throws Exception {
    Resource resource;
    Long startMs;
    Long endMs;
    try {
      String resourceString = request.getParameter(RESOURCE_PARAM);
      try {
        resource = Resource.valueOf(resourceString);
      } catch (IllegalArgumentException iae) {
        throw new IllegalArgumentException("Invalid resource type " + resourceString +
                                               ". The resource type must be one of the following: CPU, DISK, NW_IN, NW_OUT");
      }
      String startMsString = request.getParameter(START_MS_PARAM);
      String endMsString = request.getParameter(END_MS_PARAM);
      startMs = startMsString == null ? -1L : Long.parseLong(startMsString);
      endMs = endMsString == null ? System.currentTimeMillis() : Long.parseLong(endMsString);
    } catch (Exception e) {
      throw new UserRequestException(e);
    }

    ModelCompletenessRequirements requirements = new ModelCompletenessRequirements(1, 0.0, false);
    ClusterModel clusterModel = _kafkaCruiseControl.clusterModel(startMs, endMs, requirements);
    List<Partition> sortedPartitions = clusterModel.replicasSortedByUtilization(resource);
    setResponseCode(response, HttpServletResponse.SC_OK);
    OutputStream out = response.getOutputStream();
    int topicNameLength = clusterModel.topics().stream().mapToInt(String::length).max().orElse(20) + 5;
    out.write(String.format("%" + topicNameLength + "s%10s%30s%20s%20s%20s%20s%n", "PARTITION", "LEADER", "FOLLOWERS",
                            "CPU (%)", "DISK (MB)", "NW_IN (KB/s)", "NW_OUT (KB/s)")
                    .getBytes(StandardCharsets.UTF_8));
    for (Partition p : sortedPartitions) {
      List<Integer> followers = p.followers().stream().map((replica) -> replica.broker().id()).collect(Collectors.toList());
      out.write(String.format("%" + topicNameLength + "s%10s%30s%19.6f%19.3f%19.3f%19.3f%n",
                              p.leader().topicPartition(),
                              p.leader().broker().id(),
                              followers,
                              p.leader().load().expectedUtilizationFor(Resource.CPU),
                              p.leader().load().expectedUtilizationFor(Resource.DISK),
                              p.leader().load().expectedUtilizationFor(Resource.NW_IN),
                              p.leader().load().expectedUtilizationFor(Resource.NW_OUT))
                      .getBytes(StandardCharsets.UTF_8));
    }
    out.flush();
  }

  private void getProposals(HttpServletRequest request, HttpServletResponse response) throws Exception {
    boolean verbose;
    boolean ignoreProposalCache;
    boolean withAvailableValidWindows;
    boolean withAvailableValidPartitions;
    List<String> goals;
    try {
      String verboseString = request.getParameter(VERBOSE_PARAM);
      verbose = verboseString != null && Boolean.parseBoolean(verboseString);
      String ignoreProposalCacheString = request.getParameter(IGNORE_PROPOSAL_CACHE_PARAM);
      String withAvailableValidWindowsString = request.getParameter(WITH_AVAILABLE_VALID_WINDOWS);
      String withAvailableValidPartitionsString = request.getParameter(WITH_AVAILABLE_VALID_PARTITIONS);
      String goalsString = request.getParameter(GOALS_PARAM);
      goals = goalsString == null ? Collections.emptyList() : Arrays.asList(goalsString.split(","));
      goals.removeIf(String::isEmpty);
      ignoreProposalCache = (ignoreProposalCacheString != null && Boolean.parseBoolean(ignoreProposalCacheString))
          || !goals.isEmpty();
      if (withAvailableValidWindowsString != null && withAvailableValidPartitionsString != null) {
        throw new IllegalArgumentException("Cannot specify " + WITH_AVAILABLE_VALID_PARTITIONS + " and "
                                               + WITH_AVAILABLE_VALID_WINDOWS + " at the same time.");
      }
      withAvailableValidWindows = withAvailableValidWindowsString != null && Boolean.parseBoolean(withAvailableValidWindowsString);
      withAvailableValidPartitions = withAvailableValidPartitionsString != null && Boolean.parseBoolean(withAvailableValidPartitionsString);
    } catch (Exception e) {
      throw new UserRequestException(e);
    }
    GoalsAndRequirements goalsAndRequirements =
        getGoalsAndRequirements(goals, withAvailableValidPartitions, withAvailableValidWindows, ignoreProposalCache);

    GoalOptimizer.OptimizerResult optimizerResult =
        _kafkaCruiseControl.getOptimizationProposals(goalsAndRequirements.goals(),
                                                     goalsAndRequirements.requirements());
    String loadBeforeOptimization = optimizerResult.brokerStatsBeforeOptimization().toString();
    String loadAfterOptimization = optimizerResult.brokerStatsAfterOptimization().toString();

    setResponseCode(response, HttpServletResponse.SC_OK);
    OutputStream out = response.getOutputStream();
    if (!verbose) {
      out.write(KafkaCruiseControlServletUtils.getProposalSummary(optimizerResult).getBytes(StandardCharsets.UTF_8));
    } else {
      out.write(optimizerResult.goalProposals().toString().getBytes(StandardCharsets.UTF_8));
    }
    for (Map.Entry<Goal, ClusterModelStats> entry : optimizerResult.statsByGoalPriority().entrySet()) {
      Goal goal = entry.getKey();
      out.write(String.format("%n%nStats for goal %s%s:%n", goal.name(), goalResultDescription(goal, optimizerResult))
                      .getBytes(StandardCharsets.UTF_8));
      out.write(entry.getValue().toString().getBytes(StandardCharsets.UTF_8));
    }
    out.write(String.format("%n%nCurrent load:").getBytes(StandardCharsets.UTF_8));
    out.write(loadBeforeOptimization.getBytes(StandardCharsets.UTF_8));
    out.write(String.format("%n%nOptimized load:").getBytes(StandardCharsets.UTF_8));
    out.write(loadAfterOptimization.getBytes(StandardCharsets.UTF_8));
    out.flush();
  }

  private String goalResultDescription(Goal goal, GoalOptimizer.OptimizerResult optimizerResult) {
    if (optimizerResult.violatedGoalsBeforeOptimization().contains(goal)) {
      if (optimizerResult.violatedGoalsAfterOptimization().contains(goal)) {
        return "(VIOLATED)";
      } else {
        return "(FIXED)";
      }
    } else {
      return "";
    }
  }

  private ModelCompletenessRequirements getRequirements(boolean withAvailableValidPartitions,
                                                        boolean withAvailableValidWindows) {
    ModelCompletenessRequirements requirements = null;
    if (withAvailableValidPartitions) {
      requirements = new ModelCompletenessRequirements(Integer.MAX_VALUE, 0.0, true);
    } else if (withAvailableValidWindows) {
      requirements = new ModelCompletenessRequirements(1, 1.0, true);
    }
    return requirements;
  }

  private void getState(HttpServletRequest request, HttpServletResponse response) throws Exception {
    boolean verbose;
    boolean superVerbose;
    try {
      String verboseString = request.getParameter(VERBOSE_PARAM);
      verbose = verboseString != null && Boolean.parseBoolean(verboseString);
      String superVerboseString = request.getParameter(SUPER_VERBOSE_PARM);
      superVerbose = superVerboseString != null && Boolean.parseBoolean(superVerboseString);
    } catch (Exception e) {
      throw new UserRequestException(e);
    }
    KafkaCruiseControlState state = _kafkaCruiseControl.state();
    String stateString = state.toString();
    setResponseCode(response, HttpServletResponse.SC_OK);
    OutputStream out = response.getOutputStream();
    out.write(stateString.getBytes(StandardCharsets.UTF_8));
    if (verbose || superVerbose) {
      out.write(String.format("%n%nMonitored Windows:%n").getBytes(StandardCharsets.UTF_8));
      StringJoiner joiner = new StringJoiner(", ", "{", "}");
      for (Map.Entry<Long, Double> entry : state.monitorState().monitoredSnapshotWindows().entrySet()) {
        joiner.add(String.format("%d=%.3f%%", entry.getKey(), entry.getValue() * 100));
      }
      out.write(joiner.toString().getBytes(StandardCharsets.UTF_8));
      out.write(String.format("%n%nGoal Readiness:%n").getBytes(StandardCharsets.UTF_8));
      for (Map.Entry<Goal, Boolean> entry : state.analyzerState().readyGoals().entrySet()) {
        Goal goal = entry.getKey();
        out.write(String.format("%50s, %s, %s%n", goal.getClass().getSimpleName(), goal.clusterModelCompletenessRequirements(),
            entry.getValue() ? "Ready" : "NotReady").getBytes(StandardCharsets.UTF_8));
      }
      if (superVerbose) {
        out.write(String.format("%n%nFlawed metric samples:%n").getBytes(StandardCharsets.UTF_8));
        Map<TopicPartition, List<MetricSampleAggregationResult.SampleFlaw>> sampleFlaws = state.monitorState().sampleFlaws();
        if (sampleFlaws != null && !sampleFlaws.isEmpty()) {
          for (Map.Entry<TopicPartition, List<MetricSampleAggregationResult.SampleFlaw>> entry : sampleFlaws.entrySet()) {
            out.write(String.format("%n%s: %s", entry.getKey(), entry.getValue()).getBytes(StandardCharsets.UTF_8));
          }
        } else {
          out.write("None".getBytes(StandardCharsets.UTF_8));
        }
        out.write(String.format("%n%nLinear Regression Model State:%n%s", state.monitorState().detailTrainingProgress())
                        .getBytes(StandardCharsets.UTF_8));
        if (state.executorState().state() == ExecutorState.State.REPLICA_MOVEMENT_TASK_IN_PROGRESS) {
          out.write(String.format("%n%nIn progress partition movements:%n").getBytes(StandardCharsets.UTF_8));
          for (ExecutionTask task : state.executorState().inProgressPartitionMovements()) {
            out.write(String.format("%s%n", task).getBytes(StandardCharsets.UTF_8));
          }
          out.write(String.format("%n%nPending partition movements:%n").getBytes(StandardCharsets.UTF_8));
          for (ExecutionTask task : state.executorState().pendingPartitionMovements()) {
            out.write(String.format("%s%n", task).getBytes(StandardCharsets.UTF_8));
          }
        }
      }
    }
    response.getOutputStream().flush();
  }

  private void addOrRemoveBroker(HttpServletRequest request, HttpServletResponse response, EndPoint endPoint)
      throws KafkaCruiseControlException, IOException {
    List<Integer> brokerIds = new ArrayList<>();
    boolean dryrun;
    boolean withAvailableValidWindows;
    boolean withAvailableValidPartitions;
    boolean throttleRemovedBroker;
    List<String> goals;
    try {
      String[] brokerIdsString = request.getParameter(BROKER_ID_PARAM).split(",");
      for (String brokerIdString : brokerIdsString) {
        brokerIds.add(Integer.parseInt(brokerIdString));
      }
      String dryrunString = request.getParameter(DRY_RUN_PARAM);
      dryrun = dryrunString == null || Boolean.parseBoolean(dryrunString);
      String throttleRemovedBrokerString = request.getParameter(THROTTLE_REMOVED_BROKER_PARAM);
      throttleRemovedBroker = throttleRemovedBrokerString == null || Boolean.parseBoolean(throttleRemovedBrokerString);
      String goalsString = request.getParameter(GOALS_PARAM);
      goals = goalsString == null ? Collections.emptyList() : Arrays.asList(goalsString.split(","));
      goals.removeIf(String::isEmpty);
      String withAvailableValidWindowsString = request.getParameter(WITH_AVAILABLE_VALID_WINDOWS);
      String withAvailableValidPartitionsString = request.getParameter(WITH_AVAILABLE_VALID_PARTITIONS);
      if (withAvailableValidWindowsString != null && withAvailableValidPartitionsString != null) {
        throw new IllegalArgumentException("Cannot specify " + WITH_AVAILABLE_VALID_PARTITIONS + " and "
                                               + WITH_AVAILABLE_VALID_WINDOWS + " at the same time.");
      }
      withAvailableValidWindows = withAvailableValidWindowsString != null && Boolean.parseBoolean(withAvailableValidWindowsString);
      withAvailableValidPartitions = withAvailableValidPartitionsString != null && Boolean.parseBoolean(withAvailableValidPartitionsString);
    } catch (Exception e) {
      throw new UserRequestException(e);
    }
    GoalsAndRequirements goalsAndRequirements =
        getGoalsAndRequirements(goals, withAvailableValidPartitions, withAvailableValidWindows, false);
    GoalOptimizer.OptimizerResult optimizerResult;
    if (endPoint == EndPoint.ADD_BROKER) {
      optimizerResult = _kafkaCruiseControl.addBrokers(brokerIds, dryrun, throttleRemovedBroker,
                                                       goalsAndRequirements.goals(),
                                                       goalsAndRequirements.requirements());
    } else {
      optimizerResult = _kafkaCruiseControl.decommissionBrokers(brokerIds, dryrun, throttleRemovedBroker,
                                                                goalsAndRequirements.goals(),
                                                                goalsAndRequirements.requirements());
    }

    setResponseCode(response, HttpServletResponse.SC_OK);
    OutputStream out = response.getOutputStream();
    out.write(KafkaCruiseControlServletUtils.getProposalSummary(optimizerResult)
                                            .getBytes(StandardCharsets.UTF_8));
    for (Map.Entry<Goal, ClusterModelStats> entry : optimizerResult.statsByGoalPriority().entrySet()) {
      Goal goal = entry.getKey();
      out.write(String.format("%n%nStats for goal %s%s:%n", goal.name(), goalResultDescription(goal, optimizerResult))
                      .getBytes(StandardCharsets.UTF_8));
      out.write(entry.getValue().toString().getBytes(StandardCharsets.UTF_8));
    }
    out.write(String.format("%nCluster load after %s broker %s:%n",
                            endPoint == EndPoint.ADD_BROKER ? "adding" : "removing", brokerIds)
                    .getBytes(StandardCharsets.UTF_8));
    out.write(optimizerResult.brokerStatsAfterOptimization().toString()
                             .getBytes(StandardCharsets.UTF_8));
    out.flush();
  }

  private void rebalance(HttpServletRequest request, HttpServletResponse response)
      throws KafkaCruiseControlException, IOException {
    boolean dryrun;
    boolean withAvailableValidWindows;
    boolean withAvailableValidPartitions;
    List<String> goals;
    try {
      String dryrunString = request.getParameter(DRY_RUN_PARAM);
      dryrun = dryrunString == null || Boolean.parseBoolean(dryrunString);

      String goalsString = request.getParameter(GOALS_PARAM);
      goals = goalsString == null ? new ArrayList<>() : Arrays.asList(goalsString.split(","));
      goals.removeIf(String::isEmpty);

      String withAvailableValidWindowsString = request.getParameter(WITH_AVAILABLE_VALID_WINDOWS);
      String withAvailableValidPartitionsString = request.getParameter(WITH_AVAILABLE_VALID_PARTITIONS);
      if (withAvailableValidWindowsString != null && withAvailableValidPartitionsString != null) {
        throw new IllegalArgumentException("Cannot specify " + WITH_AVAILABLE_VALID_PARTITIONS + " and "
                                               + WITH_AVAILABLE_VALID_WINDOWS + " at the same time.");
      }
      withAvailableValidWindows = withAvailableValidWindowsString != null && Boolean.parseBoolean(withAvailableValidWindowsString);
      withAvailableValidPartitions = withAvailableValidPartitionsString != null && Boolean.parseBoolean(withAvailableValidPartitionsString);
    } catch (Exception e) {
      throw new UserRequestException(e);
    }
    GoalsAndRequirements goalsAndRequirements =
        getGoalsAndRequirements(goals, withAvailableValidPartitions, withAvailableValidWindows, false);
    GoalOptimizer.OptimizerResult optimizerResult = _kafkaCruiseControl.rebalance(goalsAndRequirements.goals(),
                                                                                  dryrun,
                                                                                  goalsAndRequirements.requirements());

    setResponseCode(response, HttpServletResponse.SC_OK);
    OutputStream out = response.getOutputStream();
    out.write(KafkaCruiseControlServletUtils.getProposalSummary(optimizerResult).getBytes(StandardCharsets.UTF_8));
    for (Map.Entry<Goal, ClusterModelStats> entry : optimizerResult.statsByGoalPriority().entrySet()) {
      Goal goal = entry.getKey();
      out.write(String.format("%n%nStats for goal %s%s:%n", goal.name(), goalResultDescription(goal, optimizerResult))
                      .getBytes(StandardCharsets.UTF_8));
      out.write(entry.getValue().toString().getBytes(StandardCharsets.UTF_8));
    }
    out.write(String.format("%nCluster load after rebalance:%n").getBytes(StandardCharsets.UTF_8));
    out.write(optimizerResult.brokerStatsAfterOptimization().toString().getBytes(StandardCharsets.UTF_8));
    out.flush();
  }

  private void stopProposalExecution() {
    _kafkaCruiseControl.stopProposalExecution();
  }

  private EndPoint endPoint(HttpServletRequest request) {
    String path = request.getRequestURI().toUpperCase().replace("/KAFKACRUISECONTROL/", "");
    for (EndPoint endPoint : EndPoint.values()) {
      if (endPoint.toString().equalsIgnoreCase(path)) {
        return endPoint;
      }
    }
    return null;
  }

  private void setResponseCode(HttpServletResponse response, int code) {
    response.setContentType("text/plain");
    response.setCharacterEncoding("utf-8");
    response.setStatus(code);
  }

  private void returnErrorMessage(HttpServletResponse response, String errorMessage, int responseCode)
      throws IOException {
    setResponseCode(response, responseCode);
    response.setContentLength(errorMessage.length());
    response.getOutputStream().write(errorMessage.getBytes(StandardCharsets.UTF_8));
    response.getOutputStream().flush();
  }

  private String getStateCheckUrl(HttpServletRequest request) {
    String url = request.getRequestURL().toString();
    int pos = url.indexOf("/kafkacruisecontrol/");
    return url.substring(0, pos + "/kafkacruisecontrol/".length()) + "state";
  }

  private String urlEncode(String s) throws UnsupportedEncodingException {
    return s == null ? null : URLEncoder.encode(s, StandardCharsets.UTF_8.name());
  }

  private GoalsAndRequirements getGoalsAndRequirements(List<String> userProvidedGoals,
                                                       boolean withAvailableValidPartitions,
                                                       boolean withAvailableValidWindows,
                                                       boolean ignoreCache) {
    if (!userProvidedGoals.isEmpty() || withAvailableValidPartitions || withAvailableValidWindows) {
      return new GoalsAndRequirements(userProvidedGoals,
                                      getRequirements(withAvailableValidPartitions, withAvailableValidWindows));
    }
    KafkaCruiseControlState state = _kafkaCruiseControl.state();
    int availableWindows = state.monitorState().numValidSnapshotWindows();
    List<String> allGoals = new ArrayList<>();
    List<String> readyGoals = new ArrayList<>();
    _kafkaCruiseControl.state().analyzerState().readyGoals().forEach((goal, ready) -> {
      allGoals.add(goal.name());
      if (ready) {
        readyGoals.add(goal.name());
      }
    });
    if (allGoals.size() == readyGoals.size()) {
      // If all the goals work, use it.
      return new GoalsAndRequirements(ignoreCache ? allGoals : Collections.emptyList(), null);
    } else if (availableWindows > 0) {
      // If some valid windows are available, use it.
      return new GoalsAndRequirements(ignoreCache ? allGoals : Collections.emptyList(), getRequirements(false, true));
    } else if (readyGoals.size() > 0) {
      // If no window is valid but some goals are ready, use them.
      return new GoalsAndRequirements(readyGoals, getRequirements(false, false));
    } else {
      // Ok, use default setting and let it throw exception.
      return new GoalsAndRequirements(Collections.emptyList(), null);
    }
  }

  private static class GoalsAndRequirements {
    private final List<String> _goals;
    private final ModelCompletenessRequirements _requirements;

    private GoalsAndRequirements(List<String> goals, ModelCompletenessRequirements requirements) {
      _goals = goals;
      _requirements = requirements;
    }

    private List<String> goals() {
      return _goals;
    }

    private ModelCompletenessRequirements requirements() {
      return _requirements;
    }
  }
}
