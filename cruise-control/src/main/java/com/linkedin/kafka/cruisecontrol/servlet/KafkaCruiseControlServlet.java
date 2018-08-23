/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.linkedin.kafka.cruisecontrol.KafkaClusterState;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlState;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Partition;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.async.AsyncKafkaCruiseControl;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import static com.linkedin.kafka.cruisecontrol.servlet.EndPoint.*;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.*;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND;
import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import static javax.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR;


/**
 * The servlet for Kafka Cruise Control.
 */
public class KafkaCruiseControlServlet extends HttpServlet {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaCruiseControlServlet.class);
  private static final Logger ACCESS_LOG = LoggerFactory.getLogger("CruiseControlPublicAccessLogger");

  private static final int JSON_VERSION = 1;
  private final AsyncKafkaCruiseControl _asyncKafkaCruiseControl;
  private final SessionManager _sessionManager;
  private final long _maxBlockMs;
  private final ThreadLocal<Integer> _asyncOperationStep;
  private final Map<EndPoint, Meter> _requestMeter = new HashMap<>();
  private final Map<EndPoint, Timer> _successfulRequestExecutionTimer = new HashMap<>();

  public KafkaCruiseControlServlet(AsyncKafkaCruiseControl asynckafkaCruiseControl,
                                   long maxBlockMs,
                                   long sessionExpiryMs,
                                   MetricRegistry dropwizardMetricRegistry) {
    _asyncKafkaCruiseControl = asynckafkaCruiseControl;
    _sessionManager = new SessionManager(5, sessionExpiryMs, Time.SYSTEM, dropwizardMetricRegistry, _successfulRequestExecutionTimer);
    _maxBlockMs = maxBlockMs;
    _asyncOperationStep = new ThreadLocal<>();
    _asyncOperationStep.set(0);

    for (EndPoint endpoint : EndPoint.cachedValues()) {
      _requestMeter.put(endpoint, dropwizardMetricRegistry.meter(MetricRegistry.name("KafkaCruiseControlServlet", endpoint.name() + "-request-rate")));
      _successfulRequestExecutionTimer.put(endpoint, dropwizardMetricRegistry.timer(MetricRegistry.name("KafkaCruiseControlServlet",
                                  endpoint.name() + "-successful-request-execution-timer")));
    }
  }

  @Override
  public void destroy() {
    super.destroy();
    _sessionManager.close();
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
   *    &amp;goals=[goal1,goal2...]&amp;data_from=[valid_windows/valid_partitions]
   *
   * 6. query the state of Kafka Cruise Control
   *    GET /kafkacruisecontrol/state
   *
   * 7. query the Kafka cluster state
   *    GET /kafkacruisecontrol/kafka_cluster_state
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
        if (validParamNames != null) {
          userParams.addAll(request.getParameterMap().keySet());
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
              if (getClusterLoad(request, response)) {
                _sessionManager.closeSession(request, false);
              }
              break;
            case PARTITION_LOAD:
              if (getPartitionLoad(request, response)) {
                _sessionManager.closeSession(request, false);
              }
              break;
            case PROPOSALS:
              if (getProposals(request, response)) {
                _sessionManager.closeSession(request, false);
              }
              break;
            case STATE:
              if (getState(request, response)) {
                _sessionManager.closeSession(request, false);
              }
              break;
            case KAFKA_CLUSTER_STATE:
              getKafkaClusterState(request, response);
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
      _sessionManager.closeSession(request, true);
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);
      String errorMessage = String.format("Error processing GET request '%s' due to '%s'.", request.getPathInfo(), e.getMessage());
      LOG.error(errorMessage, e);
      setErrorResponse(response, sw.toString(), errorMessage, SC_INTERNAL_SERVER_ERROR, wantJSON(request));
      _sessionManager.closeSession(request, true);
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
   *    &amp;json=[true/false]&amp;skip_hard_goal_check=[true/false]
   *
   * 2. Add a broker
   *    POST /kafkacruisecontrol/add_broker?brokerid=[id1,id2...]&amp;dryrun=[true/false]&amp;throttle_added_broker=[true/false]&amp;goals=[goal1,goal2...]
   *    &amp;allow_capacity_estimation=[true/false]&amp;concurrent_partition_movements_per_broker=[true/false]&amp;concurrent_leader_movements=[true/false]
   *    &amp;json=[true/false]&amp;skip_hard_goal_check=[true/false]
   *
   * 3. Trigger a workload balance.
   *    POST /kafkacruisecontrol/rebalance?dryrun=[true/false]&amp;force=[true/false]&amp;goals=[goal1,goal2...]&amp;allow_capacity_estimation=[true/false]
   *    &amp;concurrent_partition_movements_per_broker=[true/false]&amp;concurrent_leader_movements=[true/false]&amp;json=[true/false]
   *    &amp;skip_hard_goal_check=[true/false]
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
   *    &amp;allow_capacity_estimation=[true/false]&amp;json=[true/false]
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
              if (addOrRemoveBroker(request, response, endPoint)) {
                _sessionManager.closeSession(request, false);
              }
              break;
            case REBALANCE:
              if (rebalance(request, response, endPoint)) {
                _sessionManager.closeSession(request, false);
              }
              break;
            case STOP_PROPOSAL_EXECUTION:
              stopProposalExecution();
              setSuccessResponse(response, "Proposal execution stopped.", wantJSON(request));
              _successfulRequestExecutionTimer.get(endPoint).update(System.nanoTime() - requestExecutionStartTime, TimeUnit.NANOSECONDS);
              break;
            case PAUSE_SAMPLING:
              pauseSampling();
              setSuccessResponse(response, "Metric sampling paused.", wantJSON(request));
              _successfulRequestExecutionTimer.get(endPoint).update(System.nanoTime() - requestExecutionStartTime, TimeUnit.NANOSECONDS);
              break;
            case RESUME_SAMPLING:
              resumeSampling();
              setSuccessResponse(response, "Metric sampling resumed.", wantJSON(request));
              _successfulRequestExecutionTimer.get(endPoint).update(System.nanoTime() - requestExecutionStartTime, TimeUnit.NANOSECONDS);
              break;
            case DEMOTE_BROKER:
              if (demoteBroker(request, response, endPoint)) {
                _sessionManager.closeSession(request, false);
              }
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
      _sessionManager.closeSession(request, true);
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);
      String errorMessage = String.format("Error processing POST request '%s' due to: '%s'.", request.getPathInfo(), e.getMessage());
      LOG.error(errorMessage, e);
      setErrorResponse(response, sw.toString(), errorMessage, SC_INTERNAL_SERVER_ERROR, wantJSON(request));
      _sessionManager.closeSession(request, true);
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

    String msg = String.format("Bootstrap started. Check status through %s", getStateCheckUrl(request));
    setSuccessResponse(response, msg, json);
  }

  private void setErrorResponse(HttpServletResponse response,
                                String stackTrace,
                                String errorMessage,
                                int responseCode,
                                boolean json)
      throws IOException {
    String resp;
    if (json) {
      Map<String, Object> exceptionMap = new HashMap<>();
      exceptionMap.put("version", JSON_VERSION);
      exceptionMap.put("stackTrace", stackTrace);
      exceptionMap.put("errorMessage", errorMessage);
      Gson gson = new Gson();
      resp = gson.toJson(exceptionMap);
    } else {
      resp = errorMessage == null ? "" : errorMessage;
    }
    setResponseCode(response, responseCode, json);
    response.setContentLength(resp.length());
    response.getOutputStream().write(resp.getBytes(StandardCharsets.UTF_8));
    response.getOutputStream().flush();
  }

  private void setSuccessResponse(HttpServletResponse response,
                                  String message,
                                  boolean json)
      throws IOException {
    String resp;
    if (json) {
      Map<String, Object> respMap = new HashMap<>();
      respMap.put("version", JSON_VERSION);
      respMap.put("Message", message);
      Gson gson = new Gson();
      resp = gson.toJson(respMap);
    } else {
      resp = message == null ? "" : message;
    }
    setResponseCode(response, SC_OK, json);
    response.setContentLength(resp.length());
    response.getOutputStream().write(resp.getBytes(StandardCharsets.UTF_8));
    response.getOutputStream().flush();
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
    String message = String.format("Load model training started. Check status through %s", getStateCheckUrl(request));
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
    setResponseCode(response, SC_OK, json);
    response.setContentLength(brokerLoad.length());
    response.getOutputStream().write(brokerLoad.getBytes(StandardCharsets.UTF_8));
    response.getOutputStream().flush();
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
    List<Partition> sortedPartitions = clusterModel.replicasSortedByUtilization(resource);
    OutputStream out = response.getOutputStream();

    int numEntries = 0;
    setResponseCode(response, SC_OK, json);
    boolean wantMaxLoad = wantMaxLoad(request);
    if (!json) {
      int topicNameLength = clusterModel.topics().stream().mapToInt(String::length).max().orElse(20) + 5;
      out.write(String.format("%" + topicNameLength + "s%10s%30s%20s%20s%20s%20s%20s%n", "PARTITION", "LEADER", "FOLLOWERS",
                              "CPU (%)", "DISK (MB)", "NW_IN (KB/s)", "NW_OUT (KB/s)", "MSG_IN (#/s)")
                      .getBytes(StandardCharsets.UTF_8));
      for (Partition p : sortedPartitions) {
        if ((topic != null && !topic.matcher(p.topicPartition().topic()).matches()) ||
            p.topicPartition().partition() < partitionLowerBoundary ||
            p.topicPartition().partition() > partitionUpperBoundary) {
          continue;
        }
        if (++numEntries > entries) {
          break;
        }
        List<Integer> followers = p.followers().stream().map((replica) -> replica.broker().id()).collect(Collectors.toList());
        out.write(String.format("%" + topicNameLength + "s%10s%30s%19.6f%19.3f%19.3f%19.3f%19.3f%n",
                                p.leader().topicPartition(),
                                p.leader().broker().id(),
                                followers,
                                p.leader().load().expectedUtilizationFor(Resource.CPU, wantMaxLoad),
                                p.leader().load().expectedUtilizationFor(Resource.DISK, wantMaxLoad),
                                p.leader().load().expectedUtilizationFor(Resource.NW_IN, wantMaxLoad),
                                p.leader().load().expectedUtilizationFor(Resource.NW_OUT, wantMaxLoad),
                                p.leader().load().expectedUtilizationFor(KafkaMetricDef.MESSAGE_IN_RATE, wantMaxLoad))
                        .getBytes(StandardCharsets.UTF_8));
      }
    } else {
      Map<String, Object> partitionMap = new HashMap<>();
      List<Object> partitionList = new ArrayList<>();
      partitionMap.put("version", JSON_VERSION);
      for (Partition p : sortedPartitions) {
        if ((topic != null && !topic.matcher(p.topicPartition().topic()).matches()) ||
            p.topicPartition().partition() < partitionLowerBoundary ||
            p.topicPartition().partition() > partitionUpperBoundary) {
          continue;
        }
        if (++numEntries > entries) {
          break;
        }
        List<Integer> followers = p.followers().stream().map((replica) -> replica.broker().id()).collect(Collectors.toList());
        Map<String, Object> record = new HashMap<>();
        record.put("topic", p.leader().topicPartition().topic());
        record.put("partition", p.leader().topicPartition().partition());
        record.put("leader", p.leader().broker().id());
        record.put("followers", followers);
        record.put("CPU", p.leader().load().expectedUtilizationFor(Resource.CPU, wantMaxLoad));
        record.put("DISK", p.leader().load().expectedUtilizationFor(Resource.DISK, wantMaxLoad));
        record.put("NW_IN", p.leader().load().expectedUtilizationFor(Resource.NW_IN, wantMaxLoad));
        record.put("NW_OUT", p.leader().load().expectedUtilizationFor(Resource.NW_OUT, wantMaxLoad));
        record.put("MSG_IN", p.leader().load().expectedUtilizationFor(KafkaMetricDef.MESSAGE_IN_RATE, wantMaxLoad));
        partitionList.add(record);
      }
      partitionMap.put("records", partitionList);
      Gson gson = new Gson();
      String g = gson.toJson(partitionMap);
      response.setContentLength(g.length());
      out.write(g.getBytes(StandardCharsets.UTF_8));
    }
    out.flush();
    return true;
  }

  private boolean getProposals(HttpServletRequest request, HttpServletResponse response) throws Exception {
    boolean verbose = isVerbose(request);
    boolean ignoreProposalCache;
    DataFrom dataFrom;
    List<String> goals;
    boolean json = wantJSON(request);
    try {
      goals = getGoals(request);
      dataFrom = getDataFrom(request);
      ignoreProposalCache = ignoreProposalCache(request) || !goals.isEmpty();
    } catch (Exception e) {
      handleParameterParseException(e, response, e.getMessage(), json);
      // Close session
      return true;
    }
    GoalsAndRequirements goalsAndRequirements =
        getGoalsAndRequirements(request, response, goals, dataFrom, ignoreProposalCache);
    if (goalsAndRequirements == null) {
      return false;
    }
    boolean allowCapacityEstimation = allowCapacityEstimation(request);
    // Get the optimization result asynchronously.
    GoalOptimizer.OptimizerResult optimizerResult = getAndMaybeReturnProgress(
        request, response, () -> _asyncKafkaCruiseControl.getOptimizationProposals(goalsAndRequirements.goals(),
                                                                                   goalsAndRequirements.requirements(),
                                                                                   allowCapacityEstimation));
    if (optimizerResult == null) {
      return false;
    }

    setResponseCode(response, SC_OK, json);
    OutputStream out = response.getOutputStream();

    if (!json) {
      String loadBeforeOptimization = optimizerResult.brokerStatsBeforeOptimization().toString();
      String loadAfterOptimization = optimizerResult.brokerStatsAfterOptimization().toString();
      if (!verbose) {
        out.write(
            optimizerResult.getProposalSummary().getBytes(StandardCharsets.UTF_8));
      } else {
        out.write(optimizerResult.goalProposals().toString().getBytes(StandardCharsets.UTF_8));
      }
      for (Map.Entry<Goal, ClusterModelStats> entry : optimizerResult.statsByGoalPriority().entrySet()) {
        Goal goal = entry.getKey();
        out.write(String.format("%n%nStats for goal %s%s:%n", goal.name(), goalResultDescription(goal, optimizerResult))
            .getBytes(StandardCharsets.UTF_8));
        out.write(entry.getValue().toString().getBytes(StandardCharsets.UTF_8));
      }
      // Print summary before & after optimization
      out.write(String.format("%n%nCurrent load:").getBytes(StandardCharsets.UTF_8));
      out.write(loadBeforeOptimization.getBytes(StandardCharsets.UTF_8));
      out.write(String.format("%n%nOptimized load:").getBytes(StandardCharsets.UTF_8));
      out.write(loadAfterOptimization.getBytes(StandardCharsets.UTF_8));
    } else {
      Map<String, Object> proposalMap = new HashMap<>();
      if (!verbose) {
        proposalMap.put("summary", optimizerResult.getProposalSummaryForJson());
      } else {
        proposalMap.put("proposals", optimizerResult.goalProposals().stream().map(ExecutionProposal::getJsonStructure).collect(Collectors.toList()));
      }
      // Build all the goal summary
      List<Map<String, Object>> allGoals = new ArrayList<>();
      for (Map.Entry<Goal, ClusterModelStats> entry : optimizerResult.statsByGoalPriority().entrySet()) {
        Goal goal = entry.getKey();
        String goalViolation = goalResultDescription(goal, optimizerResult);
        Map<String, Object> goalMap = new HashMap<>();
        goalMap.put("goal", goal.name());
        goalMap.put("goalViolated", goalViolation);
        goalMap.put("clusterModelStats", entry.getValue().getJsonStructure());
        allGoals.add(goalMap);
      }
      proposalMap.put("version", JSON_VERSION);
      proposalMap.put("goals", allGoals);
      proposalMap.put("loadBeforeOptimization", optimizerResult.brokerStatsBeforeOptimization().getJsonStructure());
      proposalMap.put("loadAfterOptimization", optimizerResult.brokerStatsAfterOptimization().getJsonStructure());
      Gson gson = new GsonBuilder()
                      .serializeNulls()
                      .serializeSpecialFloatingPointValues()
                      .create();
      String proposalsString = gson.toJson(proposalMap);
      out.write(proposalsString.getBytes(StandardCharsets.UTF_8));
    }
    out.flush();
    return true;
  }

  private String goalResultDescription(Goal goal, GoalOptimizer.OptimizerResult optimizerResult) {
    return optimizerResult.violatedGoalsBeforeOptimization().contains(goal) ?
           optimizerResult.violatedGoalsAfterOptimization().contains(goal) ? "(VIOLATED)" : "(FIXED)" : "(NO-ACTION)";
  }

  private ModelCompletenessRequirements getRequirements(DataFrom dataFrom) {
    if (dataFrom == DataFrom.VALID_PARTITIONS) {
      return new ModelCompletenessRequirements(Integer.MAX_VALUE, 0.0, true);
    } else {
      return new ModelCompletenessRequirements(1, 1.0, true);
    }
  }

  private void getKafkaClusterState(HttpServletRequest request, HttpServletResponse response) throws Exception {
    boolean verbose = isVerbose(request);
    boolean json = wantJSON(request);
    KafkaClusterState state = _asyncKafkaCruiseControl.kafkaClusterState();
    OutputStream out = response.getOutputStream();
    setResponseCode(response, SC_OK, json);
    if (json) {
      String stateString = state.getJSONString(JSON_VERSION, verbose);
      response.setContentLength(stateString.length());
      out.write(stateString.getBytes(StandardCharsets.UTF_8));
    } else {
      state.writeOutputStream(out, verbose);
    }
    response.getOutputStream().flush();
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
    OutputStream out = response.getOutputStream();
    setResponseCode(response, SC_OK, json);
    if (json) {
      String stateString = state.getJSONString(JSON_VERSION, verbose);
      response.setContentLength(stateString.length());
      out.write(stateString.getBytes(StandardCharsets.UTF_8));
    } else {
      state.writeOutputStream(out, verbose, superVerbose);
    }
    response.getOutputStream().flush();
    return true;
  }

  private String generateGoalAndClusterStatusAfterExecution(boolean json, GoalOptimizer.OptimizerResult optimizerResult,
      EndPoint endPoint, List<Integer> brokerIds) {
    StringBuilder sb = new StringBuilder();
    if (!json) {
      sb.append(optimizerResult.getProposalSummary());
      for (Map.Entry<Goal, ClusterModelStats> entry : optimizerResult.statsByGoalPriority().entrySet()) {
        Goal goal = entry.getKey();
        sb.append(String.format("%n%nStats for goal %s%s:%n", goal.name(), goalResultDescription(goal, optimizerResult)));
        sb.append(entry.getValue().toString());
      }
      switch (endPoint) {
        case REBALANCE:
          sb.append("%n%nCluster load after rebalance:%n");
          break;
        case ADD_BROKER:
          sb.append(String.format("%n%nCluster load after adding broker %s:%n", brokerIds));
          break;
        case REMOVE_BROKER:
          sb.append(String.format("%n%nCluster load after removing broker %s:%n", brokerIds));
          break;
        case DEMOTE_BROKER:
          sb.append(String.format("%n%nCluster load after demoting broker %s:%n", brokerIds));
          break;
        default:
          break;
      }
      sb.append(optimizerResult.brokerStatsAfterOptimization().toString());
    } else {
      Map<String, Object> retMap = new HashMap<>();

      retMap.put("proposalSummary", optimizerResult.getProposalSummaryForJson());
      List<Object> goalStatusList = new ArrayList<>();
      for (Map.Entry<Goal, ClusterModelStats> entry : optimizerResult.statsByGoalPriority().entrySet()) {
        Goal goal = entry.getKey();
        Map<String, Object> goalRecord = new HashMap<>();
        goalRecord.put("goalName", goal.name());
        goalRecord.put("status", goalResultDescription(goal, optimizerResult));
        goalRecord.put("clusterModelStats", entry.getValue().getJsonStructure());
        goalStatusList.add(goalRecord);
      }
      retMap.put("goalSummary", goalStatusList);
      retMap.put("resultingClusterLoad", optimizerResult.brokerStatsAfterOptimization().getJsonStructure());
      retMap.put("version", JSON_VERSION);
      Gson gson = new GsonBuilder()
          .serializeNulls()
          .serializeSpecialFloatingPointValues()
          .create();
      sb.append(gson.toJson(retMap));
    }
    return sb.toString();
  }

  private boolean addOrRemoveBroker(HttpServletRequest request, HttpServletResponse response, EndPoint endPoint)
      throws Exception {
    List<Integer> brokerIds;
    Integer concurrentPartitionMovements;
    Integer concurrentLeaderMovements;
    boolean dryrun;
    DataFrom dataFrom;
    boolean throttleAddedOrRemovedBrokers;
    List<String> goals;
    boolean json = wantJSON(request);
    boolean skipHardGoalCheck;
    try {
      brokerIds = brokerIds(request);
      dryrun = getDryRun(request);
      goals = getGoals(request);
      dataFrom = getDataFrom(request);
      throttleAddedOrRemovedBrokers = throttleAddedOrRemovedBrokers(request, endPoint);
      concurrentPartitionMovements = concurrentMovementsPerBroker(request, true);
      concurrentLeaderMovements = concurrentMovementsPerBroker(request, false);
      skipHardGoalCheck = skipHardGoalCheck(request);
    } catch (Exception e) {
      handleParameterParseException(e, response, e.getMessage(), json);
      // Close session
      return true;
    }
    GoalsAndRequirements goalsAndRequirements = getGoalsAndRequirements(request, response, goals, dataFrom, false);
    if (goalsAndRequirements == null) {
      return false;
    }
    // Get proposals asynchronously.
    GoalOptimizer.OptimizerResult optimizerResult;
    boolean allowCapacityEstimation = allowCapacityEstimation(request);
    if (endPoint == ADD_BROKER) {
      optimizerResult =
          getAndMaybeReturnProgress(request, response,
                                    () -> _asyncKafkaCruiseControl.addBrokers(brokerIds,
                                                                              dryrun,
                                                                              throttleAddedOrRemovedBrokers,
                                                                              goalsAndRequirements.goals(),
                                                                              goalsAndRequirements.requirements(),
                                                                              allowCapacityEstimation,
                                                                              concurrentPartitionMovements,
                                                                              concurrentLeaderMovements,
                                                                              skipHardGoalCheck));
    } else {
      optimizerResult =
          getAndMaybeReturnProgress(request, response,
                                    () -> _asyncKafkaCruiseControl.decommissionBrokers(brokerIds,
                                                                                       dryrun,
                                                                                       throttleAddedOrRemovedBrokers,
                                                                                       goalsAndRequirements.goals(),
                                                                                       goalsAndRequirements.requirements(),
                                                                                       allowCapacityEstimation,
                                                                                       concurrentPartitionMovements,
                                                                                       concurrentLeaderMovements,
                                                                                       skipHardGoalCheck));
    }
    if (optimizerResult == null) {
      return false;
    }

    setResponseCode(response, SC_OK, json);
    OutputStream out = response.getOutputStream();
    out.write(generateGoalAndClusterStatusAfterExecution(json, optimizerResult, endPoint, brokerIds).getBytes(StandardCharsets.UTF_8));
    out.flush();
    return true;
  }

  private boolean rebalance(HttpServletRequest request, HttpServletResponse response, EndPoint endPoint) throws Exception {
    boolean dryrun;
    DataFrom dataFrom;
    List<String> goals;
    Integer concurrentPartitionMovements;
    Integer concurrentLeaderMovements;
    boolean json = wantJSON(request);
    boolean skipHardGoalCheck;
    try {
      dryrun = getDryRun(request);
      goals = getGoals(request);
      dataFrom = getDataFrom(request);
      concurrentPartitionMovements = concurrentMovementsPerBroker(request, true);
      concurrentLeaderMovements = concurrentMovementsPerBroker(request, false);
      skipHardGoalCheck = skipHardGoalCheck(request);
    } catch (Exception e) {
      handleParameterParseException(e, response, e.getMessage(), json);
      // Close session
      return true;
    }
    GoalsAndRequirements goalsAndRequirements = getGoalsAndRequirements(request, response, goals, dataFrom, false);
    if (goalsAndRequirements == null) {
      return false;
    }
    boolean allowCapacityEstimation = allowCapacityEstimation(request);
    GoalOptimizer.OptimizerResult optimizerResult =
        getAndMaybeReturnProgress(request, response,
                                  () -> _asyncKafkaCruiseControl.rebalance(goalsAndRequirements.goals(),
                                                                           dryrun,
                                                                           goalsAndRequirements.requirements(),
                                                                           allowCapacityEstimation,
                                                                           concurrentPartitionMovements,
                                                                           concurrentLeaderMovements,
                                                                           skipHardGoalCheck));
    if (optimizerResult == null) {
      return false;
    }

    setResponseCode(response, SC_OK, false);
    OutputStream out = response.getOutputStream();
    out.write(generateGoalAndClusterStatusAfterExecution(json, optimizerResult, endPoint, null).getBytes(StandardCharsets.UTF_8));
    out.flush();
    return true;
  }

  private void handleParameterParseException(Exception e, HttpServletResponse response, String errorMsg, boolean json)
      throws IOException {
    StringWriter sw = new StringWriter();
    e.printStackTrace(new PrintWriter(sw));
    setErrorResponse(response, sw.toString(), errorMsg, SC_BAD_REQUEST, json);
  }

    private boolean demoteBroker(HttpServletRequest request, HttpServletResponse response, EndPoint endPoint) throws Exception {
    List<Integer> brokerIds;
    boolean dryrun;
    Integer concurrentLeaderMovements;
    boolean json = wantJSON(request);
    try {
      brokerIds = brokerIds(request);
      dryrun = getDryRun(request);
      concurrentLeaderMovements = concurrentMovementsPerBroker(request, false);
    } catch (Exception e) {
      handleParameterParseException(e, response, e.getMessage(), json);
      // Close session
      return true;
    }

    // Get proposals asynchronously.
    boolean allowCapacityEstimation = allowCapacityEstimation(request);
    GoalOptimizer.OptimizerResult optimizerResult =
          getAndMaybeReturnProgress(request, response,
                                    () -> _asyncKafkaCruiseControl.demoteBrokers(brokerIds,
                                                                                 dryrun,
                                                                                 allowCapacityEstimation,
                                                                                 concurrentLeaderMovements));
    if (optimizerResult == null) {
      return false;
    }

    setResponseCode(response, SC_OK, false);
    OutputStream out = response.getOutputStream();
    out.write(generateGoalAndClusterStatusAfterExecution(json, optimizerResult, endPoint, brokerIds).getBytes(StandardCharsets.UTF_8));
    out.flush();
    return true;
  }

  private void stopProposalExecution() {
    _asyncKafkaCruiseControl.stopProposalExecution();
  }

  private void pauseSampling() {
    _asyncKafkaCruiseControl.pauseLoadMonitorActivity();
  }

  private void resumeSampling() {
    _asyncKafkaCruiseControl.resumeLoadMonitorActivity();
  }

  private <T> T getAndMaybeReturnProgress(HttpServletRequest request,
                                          HttpServletResponse response,
                                          Supplier<OperationFuture<T>> supplier)
      throws ExecutionException, InterruptedException, IOException {
    int step = _asyncOperationStep.get();
    OperationFuture<T> future = _sessionManager.getAndCreateSessionIfNotExist(request, supplier, step);
    _asyncOperationStep.set(step + 1);
    try {
      return future.get(_maxBlockMs, TimeUnit.MILLISECONDS);
    } catch (TimeoutException te) {
      returnProgress(response, future, wantJSON(request));
      return null;
    }
  }

  private void setResponseCode(HttpServletResponse response, int code, boolean json) {
    response.setStatus(code);
    if (json) {
      response.setContentType("application/json");
    } else {
      response.setContentType("text/plain");
    }
    response.setCharacterEncoding("utf-8");
    response.setHeader("Access-Control-Allow-Origin", "*");
    response.setHeader("Access-Control-Request-Method", "OPTIONS, GET, POST");
  }

  private void returnProgress(HttpServletResponse response, OperationFuture future, boolean json) throws IOException {
    setResponseCode(response, SC_OK, json);
    String resp;
    if (!json) {
      resp = future.progressString();
    } else {
      Gson gson = new GsonBuilder().serializeNulls().serializeSpecialFloatingPointValues().create();
      Map<String, Object> respMap = new HashMap<>();
      respMap.put("version", JSON_VERSION);
      respMap.put("progress", future.getJsonArray());
      resp = gson.toJson(respMap);
    }
    response.setContentLength(resp.length());
    response.getOutputStream().write(resp.getBytes(StandardCharsets.UTF_8));
    response.getOutputStream().flush();
  }

  private String getStateCheckUrl(HttpServletRequest request) {
    String url = request.getRequestURL().toString();
    int pos = url.indexOf("/kafkacruisecontrol/");
    return url.substring(0, pos + "/kafkacruisecontrol/".length()) + "state";
  }

  // package private for testing.
  GoalsAndRequirements getGoalsAndRequirements(HttpServletRequest request,
                                               HttpServletResponse response,
                                               List<String> userProvidedGoals,
                                               DataFrom dataFrom,
                                               boolean ignoreCache) throws Exception {
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
      return new GoalsAndRequirements(ignoreCache ? allGoals : Collections.emptyList(), getRequirements(DataFrom.VALID_WINDOWS));
    } else if (readyGoals.size() > 0) {
      // If no window is valid but some goals are ready, use them.
      return new GoalsAndRequirements(readyGoals, null);
    } else {
      // Ok, use default setting and let it throw exception.
      return new GoalsAndRequirements(Collections.emptyList(), null);
    }
  }

  static class GoalsAndRequirements {
    private final List<String> _goals;
    private final ModelCompletenessRequirements _requirements;

    private GoalsAndRequirements(List<String> goals, ModelCompletenessRequirements requirements) {
      _goals = goals;
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
