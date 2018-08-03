/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaClusterState;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlState;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Partition;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.SampleExtrapolation;
import com.linkedin.kafka.cruisecontrol.async.AsyncKafkaCruiseControl;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeMap;
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
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServlet.DataFrom.VALID_WINDOWS;
import static com.linkedin.kafka.cruisecontrol.servlet.EndPoint.*;
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
  private static final String PARTITION_MOVEMENTS = "partition movements";
  private static final String LEADERSHIP_MOVEMENTS = "leadership movements";
  private final AsyncKafkaCruiseControl _asyncKafkaCruiseControl;
  private final SessionManager _sessionManager;
  private final long _maxBlockMs;
  private final ThreadLocal<Integer> _asyncOperationStep;

  public KafkaCruiseControlServlet(AsyncKafkaCruiseControl asynckafkaCruiseControl,
                                   long maxBlockMs,
                                   long sessionExpiryMs,
                                   MetricRegistry dropwizardMetricRegistry) {
    _asyncKafkaCruiseControl = asynckafkaCruiseControl;
    _sessionManager = new SessionManager(5, sessionExpiryMs, Time.SYSTEM, dropwizardMetricRegistry);
    _maxBlockMs = maxBlockMs;
    _asyncOperationStep = new ThreadLocal<>();
    _asyncOperationStep.set(0);
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
    ACCESS_LOG.info("Received {}, {} from {}", KafkaCruiseControlServletUtils.urlEncode(request.toString()),
                    KafkaCruiseControlServletUtils.urlEncode(request.getRequestURL().toString()),
                    KafkaCruiseControlServletUtils.getClientIpAddress(request));
    try {
      _asyncOperationStep.set(0);
      EndPoint endPoint = KafkaCruiseControlServletUtils.endPoint(request);
      if (endPoint != null) {
        Set<String> validParamNames = KafkaCruiseControlServletUtils.VALID_ENDPOINT_PARAM_NAMES.get(endPoint);
        Set<String> userParams = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        if (validParamNames != null) {
          userParams.addAll(request.getParameterMap().keySet());
          userParams.removeAll(validParamNames);
        }
        if (!userParams.isEmpty()) {
          // User request specifies parameters that are not a subset of the valid parameters.
          String errorResp = String.format("Unrecognized endpoint parameters in %s get request: %s.",
                                           endPoint, userParams.toString());
          setErrorResponse(response, "", errorResp, SC_BAD_REQUEST, KafkaCruiseControlServletUtils.wantJSON(request));
        } else {
          switch (endPoint) {
            case BOOTSTRAP:
              bootstrap(request, response);
              break;
            case TRAIN:
              train(request, response);
              break;
            case LOAD:
              if (getClusterLoad(request, response)) {
                _sessionManager.closeSession(request);
              }
              break;
            case PARTITION_LOAD:
              if (getPartitionLoad(request, response)) {
                _sessionManager.closeSession(request);
              }
              break;
            case PROPOSALS:
              if (getProposals(request, response)) {
                _sessionManager.closeSession(request);
              }
              break;
            case STATE:
              if (getState(request, response)) {
                _sessionManager.closeSession(request);
              }
              break;
            case KAFKA_CLUSTER_STATE:
              getKafkaClusterState(request, response);
              break;
            default:
              throw new UserRequestException("Invalid URL for GET");
          }
        }
      } else {
        String errorMessage = String.format("Unrecognized endpoint in GET request '%s'%nSupported GET endpoints: %s",
                                            request.getPathInfo(),
                                            EndPoint.getEndpoint());
        setErrorResponse(response, "", errorMessage, SC_NOT_FOUND, KafkaCruiseControlServletUtils.wantJSON(request));
      }
    } catch (UserRequestException ure) {
      LOG.error("Why are you failing?", ure);
      String errorMessage = String.format("Bad GET request '%s'", request.getPathInfo());
      StringWriter sw = new StringWriter();
      ure.printStackTrace(new PrintWriter(sw));
      setErrorResponse(response, sw.toString(), errorMessage, SC_BAD_REQUEST, KafkaCruiseControlServletUtils.wantJSON(request));
      _sessionManager.closeSession(request);
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);
      String errorMessage = String.format("Error processing GET request '%s' due to '%s'.", request.getPathInfo(), e.getMessage());
      LOG.error(errorMessage, e);
      setErrorResponse(response, sw.toString(), errorMessage, SC_INTERNAL_SERVER_ERROR, KafkaCruiseControlServletUtils.wantJSON(request));
      _sessionManager.closeSession(request);
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
   *    &amp;json=[true/false]
   *
   * 2. Add a broker
   *    POST /kafkacruisecontrol/add_broker?brokerid=[id1,id2...]&amp;dryrun=[true/false]&amp;throttle_added_broker=[true/false]&amp;goals=[goal1,goal2...]
   *    &amp;allow_capacity_estimation=[true/false]&amp;concurrent_partition_movements_per_broker=[true/false]&amp;concurrent_leader_movements=[true/false]
   *    &amp;json=[true/false]
   *
   * 3. Trigger a workload balance.
   *    POST /kafkacruisecontrol/rebalance?dryrun=[true/false]&amp;force=[true/false]&amp;goals=[goal1,goal2...]&amp;allow_capacity_estimation=[true/false]
   *    &amp;concurrent_partition_movements_per_broker=[true/false]&amp;concurrent_leader_movements=[true/false]&amp;json=[true/false]
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
    ACCESS_LOG.info("Received {}, {} from {}", KafkaCruiseControlServletUtils.urlEncode(request.toString()),
                    KafkaCruiseControlServletUtils.urlEncode(request.getRequestURL().toString()),
                    KafkaCruiseControlServletUtils.getClientIpAddress(request));
    try {
      _asyncOperationStep.set(0);
      EndPoint endPoint = KafkaCruiseControlServletUtils.endPoint(request);
      if (endPoint != null) {
        Set<String> validParamNames = KafkaCruiseControlServletUtils.VALID_ENDPOINT_PARAM_NAMES.get(endPoint);
        Set<String> userParams = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        if (validParamNames != null) {
          userParams.addAll(request.getParameterMap().keySet());
          userParams.removeAll(validParamNames);
        }
        if (!userParams.isEmpty()) {
          // User request specifies parameters that are not a subset of the valid parameters.
          String errorResp = String.format("Unrecognized endpoint parameters in %s post request: %s.",
                                           endPoint, userParams.toString());
          setErrorResponse(response, "", errorResp, SC_BAD_REQUEST, KafkaCruiseControlServletUtils.wantJSON(request));
        } else {
          switch (endPoint) {
            case ADD_BROKER:
            case REMOVE_BROKER:
              if (addOrRemoveBroker(request, response, endPoint)) {
                _sessionManager.closeSession(request);
              }
              break;
            case REBALANCE:
              if (rebalance(request, response, endPoint)) {
                _sessionManager.closeSession(request);
              }
              break;
            case STOP_PROPOSAL_EXECUTION:
              stopProposalExecution();
              setSuccessResponse(response, "Proposal execution stopped.", KafkaCruiseControlServletUtils.wantJSON(request));
              break;
            case PAUSE_SAMPLING:
              pauseSampling();
              setSuccessResponse(response, "Metric sampling paused.", KafkaCruiseControlServletUtils.wantJSON(request));
              break;
            case RESUME_SAMPLING:
              resumeSampling();
              setSuccessResponse(response, "Metric sampling resumed.", KafkaCruiseControlServletUtils.wantJSON(request));
              break;
            case DEMOTE_BROKER:
              if (demoteBroker(request, response, endPoint)) {
                _sessionManager.closeSession(request);
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
        setErrorResponse(response, "", errorMessage, SC_NOT_FOUND, KafkaCruiseControlServletUtils.wantJSON(request));
      }
    } catch (UserRequestException ure) {
      LOG.error("Why are you failing?", ure);
      String errorMessage = String.format("Bad POST request '%s'", request.getPathInfo());
      StringWriter sw = new StringWriter();
      ure.printStackTrace(new PrintWriter(sw));
      setErrorResponse(response, sw.toString(), errorMessage, SC_BAD_REQUEST, KafkaCruiseControlServletUtils.wantJSON(request));
      _sessionManager.closeSession(request);
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);
      String errorMessage = String.format("Error processing POST request '%s' due to: '%s'.", request.getPathInfo(), e.getMessage());
      LOG.error(errorMessage, e);
      setErrorResponse(response, sw.toString(), errorMessage, SC_INTERNAL_SERVER_ERROR, KafkaCruiseControlServletUtils.wantJSON(request));
      _sessionManager.closeSession(request);
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
    boolean json = KafkaCruiseControlServletUtils.wantJSON(request);
    try {
      startMs = KafkaCruiseControlServletUtils.startMs(request);
      endMs = KafkaCruiseControlServletUtils.endMs(request);
      clearMetrics = KafkaCruiseControlServletUtils.clearMetrics(request);
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
    boolean json = KafkaCruiseControlServletUtils.wantJSON(request);
    try {
      startMs = KafkaCruiseControlServletUtils.startMs(request);
      endMs = KafkaCruiseControlServletUtils.endMs(request);
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
    boolean json = KafkaCruiseControlServletUtils.wantJSON(request);
    try {
      time = KafkaCruiseControlServletUtils.time(request);
    } catch (Exception e) {
      handleParameterParseException(e, response, e.getMessage(), json);
      // Close session
      return true;
    }

    ModelCompletenessRequirements requirements = new ModelCompletenessRequirements(1, 0.0, true);
    boolean allowCapacityEstimation = KafkaCruiseControlServletUtils.allowCapacityEstimation(request);
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
    Pattern topic = KafkaCruiseControlServletUtils.topic(request);
    int partitionUpperBoundary;
    int partitionLowerBoundary;
    boolean json = KafkaCruiseControlServletUtils.wantJSON(request);
    String resourceString = KafkaCruiseControlServletUtils.resourceString(request);
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
      Long startMsValue = KafkaCruiseControlServletUtils.startMs(request);
      startMs = startMsValue == null ? -1L : startMsValue;
      Long endMsValue = KafkaCruiseControlServletUtils.endMs(request);
      endMs = endMsValue == null ? System.currentTimeMillis() : endMsValue;
      partitionLowerBoundary = KafkaCruiseControlServletUtils.partitionBoundary(request, false);
      partitionUpperBoundary = KafkaCruiseControlServletUtils.partitionBoundary(request, true);
      entries = KafkaCruiseControlServletUtils.entries(request);
    } catch (Exception e) {
      handleParameterParseException(e, response, e.getMessage(), json);
      // Close session
      return true;
    }

    boolean allowCapacityEstimation = KafkaCruiseControlServletUtils.allowCapacityEstimation(request);
    ModelCompletenessRequirements requirements = new ModelCompletenessRequirements(1, 0.98, false);
    // Get cluster model asynchronously.
    ClusterModel clusterModel = getAndMaybeReturnProgress(
            request, response, () -> _asyncKafkaCruiseControl.clusterModel(startMs, endMs, requirements, allowCapacityEstimation));
    if (clusterModel == null) {
      return false;
    }
    List<Partition> sortedPartitions = clusterModel.replicasSortedByUtilization(resource);
    OutputStream out = response.getOutputStream();

    int numEntries = 0;
    setResponseCode(response, SC_OK, json);
    boolean wantMaxLoad = KafkaCruiseControlServletUtils.wantMaxLoad(request);
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
    boolean verbose = KafkaCruiseControlServletUtils.isVerbose(request);
    boolean ignoreProposalCache;
    DataFrom dataFrom;
    List<String> goals;
    boolean json = KafkaCruiseControlServletUtils.wantJSON(request);
    try {
      goals = KafkaCruiseControlServletUtils.getGoals(request);
      dataFrom = KafkaCruiseControlServletUtils.getDataFrom(request);
      ignoreProposalCache = KafkaCruiseControlServletUtils.ignoreProposalCache(request) || !goals.isEmpty();
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
    boolean allowCapacityEstimation = KafkaCruiseControlServletUtils.allowCapacityEstimation(request);
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

  private void writeKafkaClusterState(OutputStream out, SortedSet<PartitionInfo> partitions, int topicNameLength)
      throws IOException {
    for (PartitionInfo partitionInfo : partitions) {
      Set<String> replicas =
          Arrays.stream(partitionInfo.replicas()).map(Node::idString).collect(Collectors.toSet());
      Set<String> inSyncReplicas =
          Arrays.stream(partitionInfo.inSyncReplicas()).map(Node::idString).collect(Collectors.toSet());
      Set<String> outOfSyncReplicas = new HashSet<>(replicas);
      outOfSyncReplicas.removeAll(inSyncReplicas);

      out.write(String.format("%" + topicNameLength + "s%10s%10s%40s%40s%30s%n",
                              partitionInfo.topic(),
                              partitionInfo.partition(),
                              partitionInfo.leader() == null ? -1 : partitionInfo.leader().id(),
                              replicas,
                              inSyncReplicas,
                              outOfSyncReplicas)
                      .getBytes(StandardCharsets.UTF_8));
    }
  }

  private void getKafkaClusterState(HttpServletRequest request, HttpServletResponse response) throws Exception {
    boolean verbose = KafkaCruiseControlServletUtils.isVerbose(request);
    boolean json = KafkaCruiseControlServletUtils.wantJSON(request);
    KafkaClusterState state = _asyncKafkaCruiseControl.kafkaClusterState();
    OutputStream out = response.getOutputStream();
    setResponseCode(response, SC_OK, json);
    if (json) {
      String stateString = state.getJSONString(JSON_VERSION, verbose);
      response.setContentLength(stateString.length());
      out.write(stateString.getBytes(StandardCharsets.UTF_8));
    } else {
      Cluster clusterState = state.kafkaCluster();
      // Brokers summary.
      SortedMap<Integer, Integer> leaderCountByBrokerId = new TreeMap<>();
      SortedMap<Integer, Integer> outOfSyncCountByBrokerId = new TreeMap<>();
      SortedMap<Integer, Integer> replicaCountByBrokerId = new TreeMap<>();

      state.populateKafkaBrokerState(leaderCountByBrokerId, outOfSyncCountByBrokerId, replicaCountByBrokerId);

      String initMessage = "Brokers with replicas:";
      out.write(String.format("%s%n%20s%20s%20s%20s%n", initMessage, "BROKER", "LEADER(S)", "REPLICAS", "OUT-OF-SYNC")
                      .getBytes(StandardCharsets.UTF_8));

      for (Integer brokerId : replicaCountByBrokerId.keySet()) {
        out.write(String.format("%20d%20d%20d%20d%n",
                                brokerId,
                                leaderCountByBrokerId.getOrDefault(brokerId, 0),
                                replicaCountByBrokerId.getOrDefault(brokerId, 0),
                                outOfSyncCountByBrokerId.getOrDefault(brokerId, 0))
                        .getBytes(StandardCharsets.UTF_8));
      }

      // Partitions summary.
      int topicNameLength = clusterState.topics().stream().mapToInt(String::length).max().orElse(20) + 5;

      initMessage = verbose ? "All Partitions in the Cluster (verbose):"
                                   : "Under Replicated and Offline Partitions in the Cluster:";
      out.write(String.format("%n%s%n%" + topicNameLength + "s%10s%10s%40s%40s%30s%n", initMessage, "TOPIC", "PARTITION",
                              "LEADER", "REPLICAS", "IN-SYNC", "OUT-OF-SYNC")
                      .getBytes(StandardCharsets.UTF_8));

      // Gather the cluster state.
      Comparator<PartitionInfo> comparator =
          Comparator.comparing(PartitionInfo::topic).thenComparingInt(PartitionInfo::partition);
      SortedSet<PartitionInfo> underReplicatedPartitions = new TreeSet<>(comparator);
      SortedSet<PartitionInfo> offlinePartitions = new TreeSet<>(comparator);
      SortedSet<PartitionInfo> otherPartitions = new TreeSet<>(comparator);

      state.populateKafkaPartitionState(underReplicatedPartitions, offlinePartitions, otherPartitions, verbose);

      // Write the cluster state.
      out.write(String.format("Offline Partitions:%n").getBytes(StandardCharsets.UTF_8));
      writeKafkaClusterState(out, offlinePartitions, topicNameLength);

      out.write(String.format("Under Replicated Partitions:%n").getBytes(StandardCharsets.UTF_8));
      writeKafkaClusterState(out, underReplicatedPartitions, topicNameLength);

      if (verbose) {
        out.write(String.format("Other Partitions:%n").getBytes(StandardCharsets.UTF_8));
        writeKafkaClusterState(out, otherPartitions, topicNameLength);
      }
    }
    response.getOutputStream().flush();
  }

  private boolean getState(HttpServletRequest request, HttpServletResponse response) throws Exception {
    boolean verbose = KafkaCruiseControlServletUtils.isVerbose(request);
    boolean superVerbose = KafkaCruiseControlServletUtils.isSuperVerbose(request);
    boolean json = KafkaCruiseControlServletUtils.wantJSON(request);

    KafkaCruiseControlState state = getAndMaybeReturnProgress(request, response, _asyncKafkaCruiseControl::state);
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
      String stateString = state.toString();
      out.write(stateString.getBytes(StandardCharsets.UTF_8));
      if (verbose || superVerbose) {
        out.write(String.format("%n%nMonitored Windows [Window End_Time=Data_Completeness]:%n").getBytes(StandardCharsets.UTF_8));
        StringJoiner joiner = new StringJoiner(", ", "{", "}");
        for (Map.Entry<Long, Float> entry : state.monitorState().monitoredWindows().entrySet()) {
          joiner.add(String.format("%d=%.3f%%", entry.getKey(), entry.getValue() * 100));
        }
        out.write(joiner.toString().getBytes(StandardCharsets.UTF_8));
        out.write(String.format("%n%nGoal Readiness:%n").getBytes(StandardCharsets.UTF_8));
        for (Map.Entry<Goal, Boolean> entry : state.analyzerState().readyGoals().entrySet()) {
          Goal goal = entry.getKey();
          out.write(String.format("%50s, %s, %s%n", goal.getClass().getSimpleName(), goal.clusterModelCompletenessRequirements(),
              entry.getValue() ? "Ready" : "NotReady").getBytes(StandardCharsets.UTF_8));
        }
        ExecutorState executorState = state.executorState();
        if (executorState.state() == ExecutorState.State.REPLICA_MOVEMENT_TASK_IN_PROGRESS
            || executorState.state() == ExecutorState.State.STOPPING_EXECUTION) {
          out.write(String.format("%n%nIn progress %s:%n", PARTITION_MOVEMENTS).getBytes(StandardCharsets.UTF_8));
          for (ExecutionTask task : executorState.inProgressPartitionMovements()) {
            out.write(String.format("%s%n", task).getBytes(StandardCharsets.UTF_8));
          }
          out.write(String.format("%n%nAborting %s:%n", PARTITION_MOVEMENTS).getBytes(StandardCharsets.UTF_8));
          for (ExecutionTask task : executorState.abortingPartitionMovements()) {
            out.write(String.format("%s%n", task).getBytes(StandardCharsets.UTF_8));
          }
          out.write(String.format("%n%nAborted %s:%n", PARTITION_MOVEMENTS).getBytes(StandardCharsets.UTF_8));
          for (ExecutionTask task : executorState.abortedPartitionMovements()) {
            out.write(String.format("%s%n", task).getBytes(StandardCharsets.UTF_8));
          }
          out.write(String.format("%n%nDead %s:%n", PARTITION_MOVEMENTS).getBytes(StandardCharsets.UTF_8));
          for (ExecutionTask task : executorState.deadPartitionMovements()) {
            out.write(String.format("%s%n", task).getBytes(StandardCharsets.UTF_8));
          }
          out.write(String.format("%n%n%s %s:%n", executorState.state() == ExecutorState.State.STOPPING_EXECUTION
                                                  ? "Cancelled" : "Pending", PARTITION_MOVEMENTS).getBytes(StandardCharsets.UTF_8));
          for (ExecutionTask task : executorState.pendingPartitionMovements()) {
            out.write(String.format("%s%n", task).getBytes(StandardCharsets.UTF_8));
          }
        } else if (executorState.state() == ExecutorState.State.LEADER_MOVEMENT_TASK_IN_PROGRESS) {
          out.write(String.format("%n%nPending %s:%n", LEADERSHIP_MOVEMENTS).getBytes(StandardCharsets.UTF_8));
          for (ExecutionTask task : executorState.pendingLeadershipMovements()) {
            out.write(String.format("%s%n", task).getBytes(StandardCharsets.UTF_8));
          }
        }
        if (superVerbose) {
          out.write(String.format("%n%nExtrapolated metric samples:%n").getBytes(StandardCharsets.UTF_8));
          Map<TopicPartition, List<SampleExtrapolation>> sampleFlaws = state.monitorState().sampleExtrapolations();
          if (sampleFlaws != null && !sampleFlaws.isEmpty()) {
            for (Map.Entry<TopicPartition, List<SampleExtrapolation>> entry : sampleFlaws.entrySet()) {
              out.write(String.format("%n%s: %s", entry.getKey(), entry.getValue()).getBytes(StandardCharsets.UTF_8));
            }
          } else {
            out.write("None".getBytes(StandardCharsets.UTF_8));
          }
          if (state.monitorState().detailTrainingProgress() != null) {
            out.write(
                String.format("%n%nLinear Regression Model State:%n%s", state.monitorState().detailTrainingProgress()).getBytes(StandardCharsets.UTF_8));
          }
        }
      }
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
    boolean json = KafkaCruiseControlServletUtils.wantJSON(request);
    try {
      brokerIds = KafkaCruiseControlServletUtils.brokerIds(request);
      dryrun = KafkaCruiseControlServletUtils.getDryRun(request);
      goals = KafkaCruiseControlServletUtils.getGoals(request);
      dataFrom = KafkaCruiseControlServletUtils.getDataFrom(request);
      throttleAddedOrRemovedBrokers = KafkaCruiseControlServletUtils.throttleAddedOrRemovedBrokers(request, endPoint);
      concurrentPartitionMovements = KafkaCruiseControlServletUtils.concurrentMovementsPerBroker(request, true);
      concurrentLeaderMovements = KafkaCruiseControlServletUtils.concurrentMovementsPerBroker(request, false);
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
    boolean allowCapacityEstimation = KafkaCruiseControlServletUtils.allowCapacityEstimation(request);
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
                                                                              concurrentLeaderMovements));
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
                                                                                       concurrentLeaderMovements));
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
    boolean json = KafkaCruiseControlServletUtils.wantJSON(request);
    try {
      dryrun = KafkaCruiseControlServletUtils.getDryRun(request);
      goals = KafkaCruiseControlServletUtils.getGoals(request);
      dataFrom = KafkaCruiseControlServletUtils.getDataFrom(request);
      concurrentPartitionMovements = KafkaCruiseControlServletUtils.concurrentMovementsPerBroker(request, true);
      concurrentLeaderMovements = KafkaCruiseControlServletUtils.concurrentMovementsPerBroker(request, false);
    } catch (Exception e) {
      handleParameterParseException(e, response, e.getMessage(), json);
      // Close session
      return true;
    }
    GoalsAndRequirements goalsAndRequirements = getGoalsAndRequirements(request, response, goals, dataFrom, false);
    if (goalsAndRequirements == null) {
      return false;
    }
    boolean allowCapacityEstimation = KafkaCruiseControlServletUtils.allowCapacityEstimation(request);
    GoalOptimizer.OptimizerResult optimizerResult =
        getAndMaybeReturnProgress(request, response,
                                  () -> _asyncKafkaCruiseControl.rebalance(goalsAndRequirements.goals(),
                                                                           dryrun,
                                                                           goalsAndRequirements.requirements(),
                                                                           allowCapacityEstimation,
                                                                           concurrentPartitionMovements,
                                                                           concurrentLeaderMovements));
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
    boolean json = KafkaCruiseControlServletUtils.wantJSON(request);
    try {
      brokerIds = KafkaCruiseControlServletUtils.brokerIds(request);
      dryrun = KafkaCruiseControlServletUtils.getDryRun(request);
      concurrentLeaderMovements = KafkaCruiseControlServletUtils.concurrentMovementsPerBroker(request, false);
    } catch (Exception e) {
      handleParameterParseException(e, response, e.getMessage(), json);
      // Close session
      return true;
    }

    // Get proposals asynchronously.
    boolean allowCapacityEstimation = KafkaCruiseControlServletUtils.allowCapacityEstimation(request);
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
      returnProgress(response, future, KafkaCruiseControlServletUtils.wantJSON(request));
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
    KafkaCruiseControlState state = getAndMaybeReturnProgress(request, response, _asyncKafkaCruiseControl::state);
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
      return new GoalsAndRequirements(ignoreCache ? allGoals : Collections.emptyList(), getRequirements(VALID_WINDOWS));
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

  enum DataFrom {
    VALID_WINDOWS, VALID_PARTITIONS
  }
}
