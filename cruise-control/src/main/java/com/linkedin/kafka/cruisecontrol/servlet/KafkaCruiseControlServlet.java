/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.linkedin.kafka.cruisecontrol.KafkaClusterState;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlState;
import com.linkedin.kafka.cruisecontrol.KafkaOptimizationResult;
import com.linkedin.kafka.cruisecontrol.KafkaPartitionLoadState;
import com.linkedin.kafka.cruisecontrol.KafkaUserTaskState;
import com.linkedin.kafka.cruisecontrol.async.AsyncKafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.*;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import static javax.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND;


/**
 * The servlet for Kafka Cruise Control.
 */
public class KafkaCruiseControlServlet extends HttpServlet {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaCruiseControlServlet.class);
  private static final Logger ACCESS_LOG = LoggerFactory.getLogger("CruiseControlPublicAccessLogger");
  private static final long MAX_ACTIVE_USER_TASKS = 5;
  private final AsyncKafkaCruiseControl _asyncKafkaCruiseControl;
  private final UserTaskManager _userTaskManager;
  private final long _maxBlockMs;
  private final ThreadLocal<Integer> _asyncOperationStep;
  private final Map<EndPoint, Meter> _requestMeter = new HashMap<>();
  private final Map<EndPoint, Timer> _successfulRequestExecutionTimer = new HashMap<>();

  public KafkaCruiseControlServlet(AsyncKafkaCruiseControl asynckafkaCruiseControl,
                                   long maxBlockMs,
                                   long sessionExpiryMs,
                                   MetricRegistry dropwizardMetricRegistry) {
    _asyncKafkaCruiseControl = asynckafkaCruiseControl;
    _userTaskManager = new UserTaskManager(sessionExpiryMs, MAX_ACTIVE_USER_TASKS, dropwizardMetricRegistry, _successfulRequestExecutionTimer);
    _maxBlockMs = maxBlockMs;
    _asyncOperationStep = new ThreadLocal<>();
    _asyncOperationStep.set(0);

    for (EndPoint endpoint : EndPoint.cachedValues()) {
      _requestMeter.put(endpoint, dropwizardMetricRegistry.meter(
          MetricRegistry.name("KafkaCruiseControlServlet", endpoint.name() + "-request-rate")));
      _successfulRequestExecutionTimer.put(endpoint, dropwizardMetricRegistry.timer(
          MetricRegistry.name("KafkaCruiseControlServlet", endpoint.name() + "-successful-request-execution-timer")));
    }
  }

  @Override
  public void destroy() {
    super.destroy();
    _userTaskManager.close();
  }

  /**
   * OPTIONS request takes care of CORS applications
   *
   * https://developer.mozilla.org/en-US/docs/Web/HTTP/Access_control_CORS#Examples_of_access_control_scenarios
   */
  protected void doOptions(HttpServletRequest request, HttpServletResponse response) {
    response.setStatus(SC_OK);
    response.setHeader("Access-Control-Allow-Origin", "*");
    response.setHeader("Access-Control-Request-Method", "OPTIONS, GET, POST");
  }

  /**
   * The GET requests can do the following:
   *
   * NOTE: ADD json=true to the query parameters to get 200/OK response in JSON format.
   *
   * <pre>
   * 1. Bootstrap the load monitor
   *    RANGE MODE:
   *      GET /kafkacruisecontrol/bootstrap?start=[START_TIMESTAMP]&amp;end=[END_TIMESTAMP]
   *    SINCE MODE:
   *      GET /kafkacruisecontrol/bootstrap?start=[START_TIMESTAMP]
   *    RECENT MODE:
   *      GET /kafkacruisecontrol/bootstrap
   *
   * 2. Train the Kafka Cruise Control linear regression model. The trained model will only be used if
   *    use.linear.regression.model is set to true.
   *    GET /kafkacruisecontrol/train?start=[START_TIMESTAMP]&amp;end=[END_TIMESTAMP]
   *
   * 3. Get the cluster load
   *    GET /kafkacruisecontrol/load?time=[TIMESTAMP]
   *
   * 4. Get the partition load sorted by the utilization of a given resource and filtered by given topic regular expression
   *    and partition number/range
   *    GET /kafkacruisecontrol/partition_load?resource=[RESOURCE]&amp;start=[START_TIMESTAMP]&amp;end=[END_TIMESTAMP]
   *    &amp;topic=[topic]&amp;partition=[partition/start_partition-end_partition]
   *
   * 5. Get an optimization proposal
   *    GET /kafkacruisecontrol/proposals?verbose=[ENABLE_VERBOSE]&amp;ignore_proposal_cache=[true/false]
   *    &amp;goals=[goal1,goal2...]&amp;data_from=[valid_windows/valid_partitions]&amp;excluded_topics=[pattern]
   *    &amp;use_ready_default_goals=[true/false]
   *
   * 6. query the state of Kafka Cruise Control
   *    GET /kafkacruisecontrol/state
   *
   * 7. query the Kafka cluster state
   *    GET /kafkacruisecontrol/kafka_cluster_state
   *
   * 8. query to get active user tasks
   *    GET /kafkacruisecontrol/user_tasks
   *
   * <b>NOTE: All the timestamps are epoch time in second granularity.</b>
   * </pre>
   */
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    ACCESS_LOG.info("Received {}, {} from {}", urlEncode(request.toString()),
                    urlEncode(request.getRequestURL().toString()),
                    getClientIpAddress(request));
    try {
      _asyncOperationStep.set(0);
      EndPoint endPoint = endPoint(request);
      if (endPoint != null) {
        Set<String> validParamNames = VALID_ENDPOINT_PARAM_NAMES.get(endPoint);
        Set<String> userParams = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        userParams.addAll(request.getParameterMap().keySet());
        if (validParamNames != null) {
          userParams.removeAll(validParamNames);
        }
        if (!userParams.isEmpty()) {
          // User request specifies parameters that are not a subset of the valid parameters.
          String errorResp = String.format("Unrecognized endpoint parameters in %s get request: %s.",
                                           endPoint, userParams.toString());
          setErrorResponse(response, "", errorResp, SC_BAD_REQUEST, wantJSON(request));
        } else {
          if (EndPoint.getEndpoint().contains(endPoint)) {
            _requestMeter.get(endPoint).mark();
          }
          long requestExecutionStartTime = System.nanoTime();
          switch (endPoint) {
            case BOOTSTRAP:
              bootstrap(request, response);
              _successfulRequestExecutionTimer.get(endPoint).update(System.nanoTime() - requestExecutionStartTime, TimeUnit.NANOSECONDS);
              break;
            case TRAIN:
              train(request, response);
              _successfulRequestExecutionTimer.get(endPoint).update(System.nanoTime() - requestExecutionStartTime, TimeUnit.NANOSECONDS);
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
            case KAFKA_CLUSTER_STATE:
              getKafkaClusterState(request, response);
              _successfulRequestExecutionTimer.get(endPoint).update(System.nanoTime() - requestExecutionStartTime, TimeUnit.NANOSECONDS);
              break;
            case USER_TASKS:
              getUserTaskState(request, response);
              _successfulRequestExecutionTimer.get(endPoint).update(System.nanoTime() - requestExecutionStartTime, TimeUnit.NANOSECONDS);
              break;
            default:
              throw new UserRequestException("Invalid URL for GET");
          }
        }
      } else {
        String errorMessage = String.format("Unrecognized endpoint in GET request '%s'%nSupported GET endpoints: %s",
                                            request.getPathInfo(),
                                            EndPoint.getEndpoint());
        setErrorResponse(response, "", errorMessage, SC_NOT_FOUND, wantJSON(request));
      }
    } catch (UserRequestException ure) {
      LOG.error("Why are you failing?", ure);
      String errorMessage = String.format("Bad GET request '%s'", request.getPathInfo());
      StringWriter sw = new StringWriter();
      ure.printStackTrace(new PrintWriter(sw));
      setErrorResponse(response, sw.toString(), errorMessage, SC_BAD_REQUEST, wantJSON(request));
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);
      String errorMessage = String.format("Error processing GET request '%s' due to '%s'.", request.getPathInfo(), e.getMessage());
      LOG.error(errorMessage, e);
      setErrorResponse(response, sw.toString(), errorMessage, SC_INTERNAL_SERVER_ERROR, wantJSON(request));
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
   *    POST /kafkacruisecontrol/remove_broker?brokerid=[id1,id2...]&amp;dryrun=[true/false]&amp;throttle_removed_broker=[true/false]&amp;goals=[goal1,goal2...]
   *    &amp;allow_capacity_estimation=[true/false]&amp;concurrent_partition_movements_per_broker=[true/false]&amp;concurrent_leader_movements=[true/false]
   *    &amp;json=[true/false]&amp;skip_hard_goal_check=[true/false]&amp;excluded_topics=[pattern]&amp;use_ready_default_goals=[true/false]
   *
   * 2. Add a broker
   *    POST /kafkacruisecontrol/add_broker?brokerid=[id1,id2...]&amp;dryrun=[true/false]&amp;throttle_added_broker=[true/false]&amp;goals=[goal1,goal2...]
   *    &amp;allow_capacity_estimation=[true/false]&amp;concurrent_partition_movements_per_broker=[true/false]&amp;concurrent_leader_movements=[true/false]
   *    &amp;json=[true/false]&amp;skip_hard_goal_check=[true/false]&amp;excluded_topics=[pattern]&amp;use_ready_default_goals=[true/false]
   *
   * 3. Trigger a workload balance.
   *    POST /kafkacruisecontrol/rebalance?dryrun=[true/false]&amp;force=[true/false]&amp;goals=[goal1,goal2...]&amp;allow_capacity_estimation=[true/false]
   *    &amp;concurrent_partition_movements_per_broker=[true/false]&amp;concurrent_leader_movements=[true/false]&amp;json=[true/false]
   *    &amp;skip_hard_goal_check=[true/false]&amp;excluded_topics=[pattern]&amp;use_ready_default_goals=[true/false]
   *
   * 4. Stop the proposal execution.
   *    POST /kafkacruisecontrol/stop_proposal_execution?json=[true/false]
   *
   * 5. Pause metrics sampling. (RUNNING -&gt; PAUSED).
   *    POST /kafkacruisecontrol/pause_sampling?json=[true/false]
   *
   * 6. Resume metrics sampling. (PAUSED -&gt; RUNNING).
   *    POST /kafkacruisecontrol/resume_sampling?json=[true/false]
   *
   * 7. Demote a broker
   *    POST /kafkacruisecontrol/demote_broker?brokerid=[id1,id2...]&amp;dryrun=[true/false]&amp;concurrent_leader_movements=[true/false]
   *    &amp;allow_capacity_estimation=[true/false]&amp;json=[true/false]&amp;excluded_topics=[pattern]
   *
<<<<<<< HEAD
   * 8. Fix offline replicas
   *    POST /kafkacruisecontrol/fix_offline_replicas?brokerid=[id1,id2...]&amp;dryrun=[true/false]&amp;goals=[goal1,goal2...]
   *    &amp;allow_capacity_estimation=[true/false]&amp;concurrent_partition_movements_per_broker=[true/false]
   *    &amp;concurrent_leader_movements=[true/false]&amp;json=[true/false]
=======
   * 8. Admin.
   *    POST /kafkacruisecontrol/admin?json=[true/false]
>>>>>>> 43c6ff5... Add anomaly detector state. (#376)
   *
   * <b>NOTE: All the timestamps are epoch time in second granularity.</b>
   * </pre>
   */
  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
    ACCESS_LOG.info("Received {}, {} from {}", urlEncode(request.toString()),
                    urlEncode(request.getRequestURL().toString()),
                    getClientIpAddress(request));
    try {
      _asyncOperationStep.set(0);
      EndPoint endPoint = endPoint(request);
      if (endPoint != null) {
        Set<String> validParamNames = VALID_ENDPOINT_PARAM_NAMES.get(endPoint);
        Set<String> userParams = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        if (validParamNames != null) {
          userParams.addAll(request.getParameterMap().keySet());
          userParams.removeAll(validParamNames);
        }
        if (!userParams.isEmpty()) {
          // User request specifies parameters that are not a subset of the valid parameters.
          String errorResp = String.format("Unrecognized endpoint parameters in %s post request: %s.",
                                           endPoint, userParams.toString());
          setErrorResponse(response, "", errorResp, SC_BAD_REQUEST, wantJSON(request));
        } else {
          if (EndPoint.postEndpoint().contains(endPoint)) {
            _requestMeter.get(endPoint).mark();
          }
          long requestExecutionStartTime = System.nanoTime();
          switch (endPoint) {
            case ADD_BROKER:
            case REMOVE_BROKER:
            case FIX_OFFLINE_REPLICAS:
              addRemoveOrFixBroker(request, response, endPoint);
              break;
            case REBALANCE:
              rebalance(request, response);
              break;
            case STOP_PROPOSAL_EXECUTION:
              stopProposalExecution(request, response);
              _successfulRequestExecutionTimer.get(endPoint).update(System.nanoTime() - requestExecutionStartTime, TimeUnit.NANOSECONDS);
              break;
            case PAUSE_SAMPLING:
              pauseSampling(request, response);
              _successfulRequestExecutionTimer.get(endPoint).update(System.nanoTime() - requestExecutionStartTime, TimeUnit.NANOSECONDS);
              break;
            case RESUME_SAMPLING:
              resumeSampling(request, response);
              _successfulRequestExecutionTimer.get(endPoint).update(System.nanoTime() - requestExecutionStartTime, TimeUnit.NANOSECONDS);
              break;
            case DEMOTE_BROKER:
              demoteBroker(request, response);
              break;
            default:
              throw new UserRequestException("Invalid URL for POST");
          }
        }
      } else {
        String errorMessage = String.format("Unrecognized endpoint in POST request '%s'%nSupported POST endpoints: %s",
                                            request.getPathInfo(),
                                            EndPoint.postEndpoint());
        setErrorResponse(response, "", errorMessage, SC_NOT_FOUND, wantJSON(request));
      }
    } catch (UserRequestException ure) {
      LOG.error("Why are you failing?", ure);
      String errorMessage = String.format("Bad POST request '%s'", request.getPathInfo());
      StringWriter sw = new StringWriter();
      ure.printStackTrace(new PrintWriter(sw));
      setErrorResponse(response, sw.toString(), errorMessage, SC_BAD_REQUEST, wantJSON(request));
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);
      String errorMessage = String.format("Error processing POST request '%s' due to: '%s'.", request.getPathInfo(), e.getMessage());
      LOG.error(errorMessage, e);
      setErrorResponse(response, sw.toString(), errorMessage, SC_INTERNAL_SERVER_ERROR, wantJSON(request));
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
    boolean json = wantJSON(request);
    try {
      startMs = startMs(request);
      endMs = endMs(request);
      clearMetrics = clearMetrics(request);
      if (startMs == null && endMs != null) {
        String errorMsg = "The start time cannot be empty when end time is specified.";
        setErrorResponse(response, "", errorMsg, SC_BAD_REQUEST, json);
        return;
      }
    } catch (Exception e) {
      handleParameterParseException(e, response, e.getMessage(), json);
      return;
    }

    if (startMs != null && endMs != null) {
      _asyncKafkaCruiseControl.bootstrapLoadMonitor(startMs, endMs, clearMetrics);
    } else if (startMs != null) {
      _asyncKafkaCruiseControl.bootstrapLoadMonitor(startMs, clearMetrics);
    } else {
      _asyncKafkaCruiseControl.bootstrapLoadMonitor(clearMetrics);
    }

    String msg = String.format("Bootstrap started. Check status through %s", getStateUrl(request));
    setSuccessResponse(response, msg, json);
  }

  private void train(HttpServletRequest request, HttpServletResponse response) throws Exception {
    Long startMs;
    Long endMs;
    boolean json = wantJSON(request);
    try {
      startMs = startMs(request);
      endMs = endMs(request);
      if (startMs == null || endMs == null) {
        throw new UserRequestException("Missing start or end parameter.");
      }

    } catch (Exception e) {
      handleParameterParseException(e, response, e.getMessage(), json);
      return;
    }
    _asyncKafkaCruiseControl.trainLoadModel(startMs, endMs);
    String message = String.format("Load model training started. Check status through %s", getStateUrl(request));
    setSuccessResponse(response, message, json);
  }

  private boolean getClusterLoad(HttpServletRequest request, HttpServletResponse response) throws Exception {
    long time;
    boolean json = wantJSON(request);
    try {
      time = time(request);
    } catch (Exception e) {
      handleParameterParseException(e, response, e.getMessage(), json);
      // Close session
      return true;
    }

    ModelCompletenessRequirements requirements = new ModelCompletenessRequirements(1, 0.0, true);
    boolean allowCapacityEstimation = allowCapacityEstimation(request);
    ClusterModel.BrokerStats brokerStats = _asyncKafkaCruiseControl.cachedBrokerLoadStats(allowCapacityEstimation);
    if (brokerStats == null) {
      // Get the broker stats asynchronously.
      brokerStats = getAndMaybeReturnProgress(
          request, response, () -> _asyncKafkaCruiseControl.getBrokerStats(time, requirements, allowCapacityEstimation));
      if (brokerStats == null) {
        return false;
      }
    }
    String brokerLoad = json ? brokerStats.getJSONString(JSON_VERSION) : brokerStats.toString();
    writeResponseToOutputStream(response, SC_OK, json, brokerLoad);
    return true;
  }

  private boolean getPartitionLoad(HttpServletRequest request, HttpServletResponse response) throws Exception {
    Resource resource;
    long startMs;
    long endMs;
    int entries;
    Pattern topic = topic(request);
    int partitionUpperBoundary;
    int partitionLowerBoundary;
    boolean json = wantJSON(request);
    Double minValidPartitionRatio;
    String resourceString = resourceString(request);
    try {
      resource = Resource.valueOf(resourceString.toUpperCase());
    } catch (IllegalArgumentException iae) {
      String errorMsg = String.format("Invalid resource type %s. The resource type must be one of the following: "
                                      + "CPU, DISK, NW_IN, NW_OUT", resourceString);
      handleParameterParseException(iae, response, errorMsg, json);
      // Close session
      return true;
    }

    try {
      Long startMsValue = startMs(request);
      startMs = startMsValue == null ? -1L : startMsValue;
      Long endMsValue = endMs(request);
      endMs = endMsValue == null ? System.currentTimeMillis() : endMsValue;
      partitionLowerBoundary = partitionBoundary(request, false);
      partitionUpperBoundary = partitionBoundary(request, true);
      entries = entries(request);
      minValidPartitionRatio = minValidPartitionRatio(request);
    } catch (Exception e) {
      handleParameterParseException(e, response, e.getMessage(), json);
      // Close session
      return true;
    }

    boolean allowCapacityEstimation = allowCapacityEstimation(request);
    // Get cluster model asynchronously.
    ClusterModel clusterModel = getAndMaybeReturnProgress(
            request, response, () -> _asyncKafkaCruiseControl.clusterModel(startMs, endMs, minValidPartitionRatio, allowCapacityEstimation));
    if (clusterModel == null) {
      return false;
    }

    int topicNameLength = clusterModel.topics().stream().mapToInt(String::length).max().orElse(20) + 5;
    KafkaPartitionLoadState kafkaPartitionLoadState = new KafkaPartitionLoadState(clusterModel.replicasSortedByUtilization(resource),
                                                                                  wantMaxLoad(request),
                                                                                  entries,
                                                                                  partitionUpperBoundary,
                                                                                  partitionLowerBoundary,
                                                                                  topic,
                                                                                  topicNameLength);
    writeSuccessResponse(response,
                         () -> kafkaPartitionLoadState.getJSONString(JSON_VERSION),
                         kafkaPartitionLoadState::writeOutputStream,
                         wantJSON(request));
    return true;
  }

  private boolean getProposals(HttpServletRequest request, HttpServletResponse response) throws Exception {
    boolean verbose = isVerbose(request);
    boolean ignoreProposalCache;
    boolean useReadyDefaultGoals;
    DataFrom dataFrom;
    List<String> goals;
    boolean json = wantJSON(request);
    Pattern excludedTopics;
    try {
      goals = getGoals(request);
      dataFrom = getDataFrom(request);
      ignoreProposalCache = ignoreProposalCache(request) || !goals.isEmpty();
      useReadyDefaultGoals = useReadyDefaultGoals(request);
      excludedTopics = excludedTopics(request);
    } catch (Exception e) {
      handleParameterParseException(e, response, e.getMessage(), json);
      // Close session
      return true;
    }
    GoalsAndRequirements goalsAndRequirements =
        getGoalsAndRequirements(request, response, goals, dataFrom, ignoreProposalCache, useReadyDefaultGoals);
    if (goalsAndRequirements == null) {
      return false;
    }
    boolean allowCapacityEstimation = allowCapacityEstimation(request);
    // Get the optimization result asynchronously.
    KafkaOptimizationResult optimizationResult = getAndMaybeReturnProgress(
        request, response, () -> _asyncKafkaCruiseControl.getOptimizationProposals(goalsAndRequirements.goals(),
                                                                                   goalsAndRequirements.requirements(),
                                                                                   allowCapacityEstimation,
                                                                                   excludedTopics));
    if (optimizationResult == null) {
      return false;
    }

    writeSuccessResponse(response,
                         () -> optimizationResult.getJSONString(JSON_VERSION, verbose),
                         out -> optimizationResult.writeOutputStream(out, verbose),
                         wantJSON(request));

    return true;
  }

  private void getKafkaClusterState(HttpServletRequest request, HttpServletResponse response) throws Exception {
    KafkaClusterState state = _asyncKafkaCruiseControl.kafkaClusterState();

    boolean verbose = isVerbose(request);
    writeSuccessResponse(response,
                         () -> state.getJSONString(JSON_VERSION, verbose),
                         out -> state.writeOutputStream(out, verbose),
                         wantJSON(request));
  }

  private boolean getState(HttpServletRequest request, HttpServletResponse response) throws Exception {
    boolean verbose = isVerbose(request);
    boolean superVerbose = isSuperVerbose(request);
    boolean json = wantJSON(request);
    Set<KafkaCruiseControlState.SubState> substates;
    try {
      substates = substates(request);
    } catch (Exception e) {
      handleParameterParseException(e, response, e.getMessage(), json);
      // Close session
      return true;
    }

    KafkaCruiseControlState state = getAndMaybeReturnProgress(request, response,
                                                              () -> _asyncKafkaCruiseControl.state(substates));
    if (state == null) {
      return false;
    }

    writeSuccessResponse(response,
                         () -> state.getJSONString(JSON_VERSION, verbose),
                         out -> state.writeOutputStream(out, verbose, superVerbose),
                         json);
    return true;
  }

  private boolean addRemoveOrFixBroker(HttpServletRequest request, HttpServletResponse response, EndPoint endPoint)
      throws Exception {
    List<Integer> brokerIds;
    Integer concurrentPartitionMovements;
    Integer concurrentLeaderMovements;
    Pattern excludedTopics;
    boolean dryrun;
    DataFrom dataFrom;
    boolean throttleAddedOrRemovedBrokers;
    List<String> goals;
    boolean json = wantJSON(request);
    boolean skipHardGoalCheck;
    boolean useReadyDefaultGoals;
    try {
      // The parameters retrieved here are supported by either ADD_BROKER, REMOVE_BROKER, or FIX_OFFLINE_REPLICAS.
      brokerIds = brokerIds(request);
      dryrun = getDryRun(request);
      goals = getGoals(request);
      dataFrom = getDataFrom(request);
      throttleAddedOrRemovedBrokers = throttleAddedOrRemovedBrokers(request, endPoint);
      concurrentPartitionMovements = concurrentMovements(request, true);
      concurrentLeaderMovements = concurrentMovements(request, false);
      useReadyDefaultGoals = useReadyDefaultGoals(request);
      skipHardGoalCheck = skipHardGoalCheck(request);
      excludedTopics = excludedTopics(request);
    } catch (Exception e) {
      handleParameterParseException(e, response, e.getMessage(), json);
      // Close session
      return true;
    }
    GoalsAndRequirements goalsAndRequirements =
        getGoalsAndRequirements(request, response, goals, dataFrom, false, useReadyDefaultGoals);
    if (goalsAndRequirements == null) {
      return false;
    }
    // Get proposals asynchronously.
    KafkaOptimizationResult optimizationResult;
    boolean allowCapacityEstimation = allowCapacityEstimation(request);
    switch (endPoint) {
      case ADD_BROKER:
        optimizationResult =
            getAndMaybeReturnProgress(request, response,
                                      () -> _asyncKafkaCruiseControl.addBrokers(brokerIds,
                                                                                dryrun,
                                                                                throttleAddedOrRemovedBrokers,
                                                                                goalsAndRequirements.goals(),
                                                                                goalsAndRequirements.requirements(),
                                                                                allowCapacityEstimation,
                                                                                concurrentPartitionMovements,
                                                                                concurrentLeaderMovements,
                                                                                skipHardGoalCheck,
                                                                                excludedTopics));
        break;
      case REMOVE_BROKER:
        optimizationResult =
            getAndMaybeReturnProgress(request, response,
                                      () -> _asyncKafkaCruiseControl.decommissionBrokers(brokerIds,
                                                                                         dryrun,
                                                                                         throttleAddedOrRemovedBrokers,
                                                                                         goalsAndRequirements.goals(),
                                                                                         goalsAndRequirements.requirements(),
                                                                                         allowCapacityEstimation,
                                                                                         concurrentPartitionMovements,
                                                                                         concurrentLeaderMovements,
                                                                                         skipHardGoalCheck,
                                                                                         excludedTopics));
        break;
      case FIX_OFFLINE_REPLICAS:
        optimizationResult = getAndMaybeReturnProgress(request, response,
                                                       () -> _asyncKafkaCruiseControl.fixOfflineReplicas(dryrun,
                                                                                                         goalsAndRequirements.goals(),
                                                                                                         goalsAndRequirements.requirements(),
                                                                                                         allowCapacityEstimation,
                                                                                                         concurrentPartitionMovements,
                                                                                                         concurrentLeaderMovements,
                                                                                                         skipHardGoalCheck,
                                                                                                         excludedTopics));
        break;
      default:
        // Should never reach here.
        throw new IllegalArgumentException("Unexpected endpoint");
    }
    if (optimizationResult == null) {
      return false;
    }

    String pretext;
    switch (endPoint) {
      case ADD_BROKER:
        pretext = String.format("%n%nCluster load after adding broker %s:%n", brokerIds);
        break;
      case REMOVE_BROKER:
        pretext = String.format("%n%nCluster load after removing broker %s:%n", brokerIds);
        break;
      case FIX_OFFLINE_REPLICAS:
        pretext = String.format("%n%nCluster load after fixing offline replicas on broker %s:%n", brokerIds);
        break;
      default:
        // Should never reach here.
        throw new IllegalArgumentException("Unexpected endpoint");
    }

    writeSuccessResponse(response,
                         () -> optimizationResult.getJSONString(JSON_VERSION),
                         out -> optimizationResult.writeOutputStream(out, pretext),
                         json);
    return true;
  }

  private boolean rebalance(HttpServletRequest request, HttpServletResponse response) throws Exception {
    boolean dryrun;
    DataFrom dataFrom;
    List<String> goals;
    Integer concurrentPartitionMovements;
    Integer concurrentLeaderMovements;
    Pattern excludedTopics;
    boolean json = wantJSON(request);
    boolean skipHardGoalCheck;
    boolean useReadyDefaultGoals;
    try {
      dryrun = getDryRun(request);
      goals = getGoals(request);
      dataFrom = getDataFrom(request);
      concurrentPartitionMovements = concurrentMovements(request, true);
      concurrentLeaderMovements = concurrentMovements(request, false);
      useReadyDefaultGoals = useReadyDefaultGoals(request);
      skipHardGoalCheck = skipHardGoalCheck(request);
      excludedTopics = excludedTopics(request);
    } catch (Exception e) {
      handleParameterParseException(e, response, e.getMessage(), json);
      // Close session
      return true;
    }
    GoalsAndRequirements goalsAndRequirements =
        getGoalsAndRequirements(request, response, goals, dataFrom, false, useReadyDefaultGoals);
    if (goalsAndRequirements == null) {
      return false;
    }
    boolean allowCapacityEstimation = allowCapacityEstimation(request);
    KafkaOptimizationResult optimizationResult =
        getAndMaybeReturnProgress(request, response,
                                  () -> _asyncKafkaCruiseControl.rebalance(goalsAndRequirements.goals(),
                                                                           dryrun,
                                                                           goalsAndRequirements.requirements(),
                                                                           allowCapacityEstimation,
                                                                           concurrentPartitionMovements,
                                                                           concurrentLeaderMovements,
                                                                           skipHardGoalCheck,
                                                                           excludedTopics));
    if (optimizationResult == null) {
      return false;
    }
    String pretext = String.format("%n%nCluster load after rebalance:%n");
    writeSuccessResponse(response,
                         () -> optimizationResult.getJSONString(JSON_VERSION),
                         out -> optimizationResult.writeOutputStream(out, pretext),
                         json);
    return true;
  }

  private boolean demoteBroker(HttpServletRequest request, HttpServletResponse response) throws Exception {
    List<Integer> brokerIds;
    boolean dryrun;
    Integer concurrentLeaderMovements;
    boolean json = wantJSON(request);
    try {
      brokerIds = brokerIds(request);
      dryrun = getDryRun(request);
      concurrentLeaderMovements = concurrentMovements(request, false);
    } catch (Exception e) {
      handleParameterParseException(e, response, e.getMessage(), json);
      // Close session
      return true;
    }

    // Get proposals asynchronously.
    boolean allowCapacityEstimation = allowCapacityEstimation(request);
    KafkaOptimizationResult optimizationResult =
        getAndMaybeReturnProgress(request, response,
                                  () -> _asyncKafkaCruiseControl.demoteBrokers(brokerIds,
                                                                               dryrun,
                                                                               allowCapacityEstimation,
                                                                               concurrentLeaderMovements));
    if (optimizationResult == null) {
      return false;
    }

    String pretext = String.format("%n%nCluster load after demoting broker %s:%n", brokerIds);
    writeSuccessResponse(response,
                         () -> optimizationResult.getJSONString(JSON_VERSION),
                         out -> optimizationResult.writeOutputStream(out, pretext),
                         json);
    return true;
  }

  private void stopProposalExecution(HttpServletRequest request, HttpServletResponse response) throws IOException {
    _asyncKafkaCruiseControl.stopProposalExecution();
    setSuccessResponse(response, "Proposal execution stopped.", wantJSON(request));
  }

  private void pauseSampling(HttpServletRequest request, HttpServletResponse response) throws IOException {
    _asyncKafkaCruiseControl.pauseLoadMonitorActivity();
    setSuccessResponse(response, "Metric sampling paused.", wantJSON(request));
  }

  private void resumeSampling(HttpServletRequest request, HttpServletResponse response) throws IOException {
    _asyncKafkaCruiseControl.resumeLoadMonitorActivity();
    setSuccessResponse(response, "Metric sampling resumed.", wantJSON(request));
  }

  private <T> T getAndMaybeReturnProgress(HttpServletRequest request,
                                          HttpServletResponse response,
                                          Supplier<OperationFuture<T>> supplier)
      throws ExecutionException, InterruptedException, IOException {
    int step = _asyncOperationStep.get();
    OperationFuture<T> future = _userTaskManager.getOrCreateUserTask(request, response, supplier, step);
    _asyncOperationStep.set(step + 1);
    try {
      return future.get(_maxBlockMs, TimeUnit.MILLISECONDS);
    } catch (TimeoutException te) {
      returnProgress(response, future, wantJSON(request));
      return null;
    }
  }

  // package private for testing.
  GoalsAndRequirements getGoalsAndRequirements(HttpServletRequest request,
                                               HttpServletResponse response,
                                               List<String> userProvidedGoals,
                                               DataFrom dataFrom,
                                               boolean ignoreCache,
                                               boolean useReadyDefaultGoals) throws Exception {
    if (!userProvidedGoals.isEmpty() || dataFrom == DataFrom.VALID_PARTITIONS) {
      return new GoalsAndRequirements(userProvidedGoals, getRequirements(dataFrom));
    }
    Set<KafkaCruiseControlState.SubState> substates = new HashSet<>(
        Arrays.asList(KafkaCruiseControlState.SubState.ANALYZER, KafkaCruiseControlState.SubState.MONITOR));

    KafkaCruiseControlState state = getAndMaybeReturnProgress(request, response,
                                                              () -> _asyncKafkaCruiseControl.state(substates));
    if (state == null) {
      return null;
    }
    int availableWindows = state.monitorState().numValidWindows();
    List<String> allGoals = new ArrayList<>();
    List<String> readyGoals = new ArrayList<>();
    state.analyzerState().readyGoals().forEach((goal, ready) -> {
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
      return new GoalsAndRequirements(ignoreCache ? allGoals : Collections.emptyList(), getRequirements(dataFrom));
    } else if (useReadyDefaultGoals && readyGoals.size() > 0) {
      // If no window is valid but some goals are ready, use them if using ready goals is permitted.
      return new GoalsAndRequirements(readyGoals, null);
    } else {
      // Ok, use default setting and let it throw exception.
      return new GoalsAndRequirements(Collections.emptyList(), null);
    }
  }

  private void getUserTaskState(HttpServletRequest request, HttpServletResponse response) throws IOException {
    KafkaUserTaskState kafkaUserTaskState = new KafkaUserTaskState(_userTaskManager.getActiveUserTasks(),
                                                                   _userTaskManager.getCompletedUserTasks());
    writeSuccessResponse(response,
                         () -> kafkaUserTaskState.getJSONString(JSON_VERSION),
                         kafkaUserTaskState::writeOutputStream,
                         wantJSON(request));
  }

  static class GoalsAndRequirements {
    private final List<String> _goals;
    private final ModelCompletenessRequirements _requirements;

    private GoalsAndRequirements(List<String> goals, ModelCompletenessRequirements requirements) {
      _goals = goals; // An empty list indicates the default goals.
      _requirements = requirements;
    }

    List<String> goals() {
      return _goals;
    }

    ModelCompletenessRequirements requirements() {
      return _requirements;
    }
  }
}
