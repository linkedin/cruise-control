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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServlet.EndPoint.*;


/**
 * The servlet for Kafka Cruise Control.
 */
public class KafkaCruiseControlServlet extends HttpServlet {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaCruiseControlServlet.class);
  private static final Logger ACCESS_LOG = LoggerFactory.getLogger("CruiseControlPublicAccessLogger");

  private static final String JSON_PARAM = "json";
  private static final String START_MS_PARAM = "start";
  private static final String END_MS_PARAM = "end";
  private static final String CLEAR_METRICS_PARAM = "clearmetrics";
  private static final String TIME_PARAM = "time";
  private static final String VERBOSE_PARAM = "verbose";
  private static final String SUPER_VERBOSE_PARM = "super_verbose";
  private static final String RESOURCE_PARAM = "resource";
  private static final String DATA_FROM_PARAM = "data_from";

  private static final String GOALS_PARAM = "goals";
  private static final String GRANULARITY_PARAM = "granularity";
  private static final String GRANULARITY_BROKER = "broker";

  private static final String GRANULARITY_REPLICA = "replica";
  private static final String BROKER_ID_PARAM = "brokerid";
  private static final String DRY_RUN_PARAM = "dryrun";
  private static final String THROTTLE_ADDED_BROKER_PARAM = "throttle_added_broker";
  private static final String THROTTLE_REMOVED_BROKER_PARAM = "throttle_removed_broker";
  private static final String IGNORE_PROPOSAL_CACHE_PARAM = "ignore_proposal_cache";

  private static final Map<EndPoint, Set<String>> VALID_ENDPOINT_PARAM_NAMES;
  static {
    Map<EndPoint, Set<String>> validParamNames = new HashMap<>();

    Set<String> bootstrap = new HashSet<>();
    bootstrap.add(START_MS_PARAM);
    bootstrap.add(END_MS_PARAM);
    bootstrap.add(CLEAR_METRICS_PARAM);
    bootstrap.add(JSON_PARAM);

    Set<String> train = new HashSet<>();
    train.add(START_MS_PARAM);
    train.add(END_MS_PARAM);
    train.add(JSON_PARAM);

    Set<String> load = new HashSet<>();
    load.add(TIME_PARAM);
    load.add(GRANULARITY_PARAM);
    load.add(JSON_PARAM);

    Set<String> partitionLoad = new HashSet<>();
    partitionLoad.add(RESOURCE_PARAM);
    partitionLoad.add(START_MS_PARAM);
    partitionLoad.add(END_MS_PARAM);
    partitionLoad.add(JSON_PARAM);

    Set<String> proposals = new HashSet<>();
    proposals.add(VERBOSE_PARAM);
    proposals.add(IGNORE_PROPOSAL_CACHE_PARAM);
    proposals.add(DATA_FROM_PARAM);
    proposals.add(GOALS_PARAM);
    proposals.add(JSON_PARAM);

    Set<String> state = new HashSet<>();
    state.add(VERBOSE_PARAM);
    state.add(SUPER_VERBOSE_PARM);
    state.add(JSON_PARAM);

    Set<String> addOrRemoveBroker = new HashSet<>();
    addOrRemoveBroker.add(BROKER_ID_PARAM);
    addOrRemoveBroker.add(DRY_RUN_PARAM);
    addOrRemoveBroker.add(THROTTLE_REMOVED_BROKER_PARAM);
    addOrRemoveBroker.add(DATA_FROM_PARAM);
    addOrRemoveBroker.add(GOALS_PARAM);
    addOrRemoveBroker.add(JSON_PARAM);

    Set<String> addBroker = new HashSet<>();
    addBroker.add(THROTTLE_ADDED_BROKER_PARAM);
    addBroker.addAll(addOrRemoveBroker);

    Set<String> removeBroker = new HashSet<>();
    removeBroker.add(THROTTLE_REMOVED_BROKER_PARAM);
    removeBroker.addAll(addOrRemoveBroker);

    Set<String> rebalance = new HashSet<>();
    rebalance.add(DRY_RUN_PARAM);
    rebalance.add(GOALS_PARAM);
    rebalance.add(DATA_FROM_PARAM);
    rebalance.add(JSON_PARAM);

    validParamNames.put(BOOTSTRAP, Collections.unmodifiableSet(bootstrap));
    validParamNames.put(TRAIN, Collections.unmodifiableSet(train));
    validParamNames.put(LOAD, Collections.unmodifiableSet(load));
    validParamNames.put(PARTITION_LOAD, Collections.unmodifiableSet(partitionLoad));
    validParamNames.put(PROPOSALS, Collections.unmodifiableSet(proposals));
    validParamNames.put(STATE, Collections.unmodifiableSet(state));
    validParamNames.put(ADD_BROKER, Collections.unmodifiableSet(addBroker));
    validParamNames.put(REMOVE_BROKER, Collections.unmodifiableSet(removeBroker));
    validParamNames.put(REBALANCE, Collections.unmodifiableSet(rebalance));
    validParamNames.put(STOP_PROPOSAL_EXECUTION, Collections.emptySet());

    VALID_ENDPOINT_PARAM_NAMES = Collections.unmodifiableMap(validParamNames);
  }

  protected enum EndPoint {
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
   * OPTIONS request takes care of CORS applications
   *
   * https://developer.mozilla.org/en-US/docs/Web/HTTP/Access_control_CORS#Examples_of_access_control_scenarios
   */
  protected void doOptions(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    response.setStatus(HttpServletResponse.SC_OK);
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
   *    &goals=[goal1,goal2...]&data_from=[valid_windows/valid_partitions]
   *
   * 6. query the state of Kafka Cruise Control
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
        Set<String> validParamNames = VALID_ENDPOINT_PARAM_NAMES.get(endPoint);
        Set<String> userParams = new HashSet<>();
        if (validParamNames != null) {
          userParams.addAll(request.getParameterMap().keySet());
          userParams.removeAll(validParamNames);
        }
        if (!userParams.isEmpty()) {
          // User request specifies parameters that are not a subset of the valid parameters.
          String errorResp;
          if (wantJSON(request)) {
            errorResp = "{\"error\": \"Unrecognized endpoint parameters in " + endPoint + " get request: " +
                userParams.toString() + "\"}";
          } else {
            errorResp = "Unrecognized endpoint parameters in " + endPoint + " get request: " +
                userParams.toString() + ".";
          }
          returnErrorMessage(response, errorResp, HttpServletResponse.SC_NOT_IMPLEMENTED);
        } else {
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
   *    POST /kafkacruisecontrol/add_broker?brokerid=[id1,id2...]&dryrun=[true/false]&throttle_added_broker=[true/false]&goals=[goal1,goal2...]
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
        Set<String> validParamNames = VALID_ENDPOINT_PARAM_NAMES.get(endPoint);
        Set<String> userParams = new HashSet<>();
        if (validParamNames != null) {
          userParams.addAll(request.getParameterMap().keySet());
          userParams.removeAll(validParamNames);
        }
        if (!userParams.isEmpty()) {
          // User request specifies parameters that are not a subset of the valid parameters.
          String errorResp;
          if (wantJSON(request)) {
            errorResp = "{\"error\": \"Unrecognized endpoint parameters in " + endPoint + " post request: " +
                userParams.toString() + "\"}";
          } else {
            errorResp = "Unrecognized endpoint parameters in " + endPoint + " post request: " +
                userParams.toString() + ".";
          }
          returnErrorMessage(response, errorResp, HttpServletResponse.SC_NOT_IMPLEMENTED);
        } else {
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
              throw new UserRequestException("Invalid URL for POST");
          }
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
    boolean json;
    try {
      String startMsString = request.getParameter(START_MS_PARAM);
      String endMsString = request.getParameter(END_MS_PARAM);
      String clearMetricsString = request.getParameter(CLEAR_METRICS_PARAM);
      startMs = startMsString == null ? null : Long.parseLong(startMsString);
      endMs = endMsString == null ? null : Long.parseLong(endMsString);
      clearMetrics = clearMetricsString == null || Boolean.parseBoolean(clearMetricsString);
      json = wantJSON(request);
      if (startMs == null && endMs != null) {
        throw new IllegalArgumentException("The start time cannot be empty when end time is specified.");
      }
    } catch (Exception e) {
      throw new UserRequestException(e);
    }

    if (startMs != null && endMs != null) {
      _kafkaCruiseControl.bootstrapLoadMonitor(startMs, endMs, clearMetrics);
    } else if (startMs != null) {
      _kafkaCruiseControl.bootstrapLoadMonitor(startMs, clearMetrics);
    } else {
      _kafkaCruiseControl.bootstrapLoadMonitor(clearMetrics);
    }
    String resp;
    if (json) {
      resp = "{\"message\": \"Bootstrap started.\"}";
      setJSONResponseCode(request, response, HttpServletResponse.SC_OK);
    } else {
      resp = "Bootstrap started. Check status through " + getStateCheckUrl(request);
      setResponseCode(response, HttpServletResponse.SC_OK);
    }
    response.setContentLength(resp.length());
    response.getOutputStream().write(resp.getBytes(StandardCharsets.UTF_8));
    response.getOutputStream().flush();
  }

  private void train(HttpServletRequest request, HttpServletResponse response) throws Exception {
    Long startMs;
    Long endMs;
    boolean json;
    try {
      String startMsString = request.getParameter(START_MS_PARAM);
      String endMsString = request.getParameter(END_MS_PARAM);
      startMs = Long.parseLong(startMsString);
      endMs = Long.parseLong(endMsString);
      json = wantJSON(request);
    } catch (Exception e) {
      throw new UserRequestException(e);
    }
    _kafkaCruiseControl.trainLoadModel(startMs, endMs);
    String resp;
    if (json) {
      resp = "{\"message\": \"Load model training started.\"}";
      setJSONResponseCode(request, response, HttpServletResponse.SC_OK);
    } else {
      resp = "Load model training started. Check status through " + getStateCheckUrl(request);
      setResponseCode(response, HttpServletResponse.SC_OK);
    }
    response.setContentLength(resp.length());
    response.getOutputStream().write(resp.getBytes(StandardCharsets.UTF_8));
    response.getOutputStream().flush();
  }

  private void getClusterLoad(HttpServletRequest request, HttpServletResponse response) throws Exception {
    long time;
    String granularity;
    boolean json = false;
    try {
      String timeString = request.getParameter(TIME_PARAM);
      time = (timeString == null || timeString.toUpperCase().equals("NOW"))
          ? System.currentTimeMillis() : Long.parseLong(timeString);
      granularity = request.getParameter(GRANULARITY_PARAM);
      json = wantJSON(request);
    } catch (Exception e) {
      throw new UserRequestException(e);
    }

    ModelCompletenessRequirements requirements = new ModelCompletenessRequirements(1, 0.0, false);
    if (granularity == null || granularity.toLowerCase().equals(GRANULARITY_BROKER)) {
      ClusterModel.BrokerStats brokerStats = _kafkaCruiseControl.cachedBrokerLoadStats();
      String brokerLoad;
      if (brokerStats != null) {
        if (json) {
          brokerLoad = brokerStats.getJSONString();
        } else {
          brokerLoad = brokerStats.toString();
        }
      } else {
        ClusterModel clusterModel = _kafkaCruiseControl.clusterModel(time, requirements);
        if (json) {
          brokerLoad = clusterModel.brokerStatsJSON();
        } else {
          brokerLoad = clusterModel.brokerStats().toString();
        }
      }
      if (json) {
        setJSONResponseCode(request, response, HttpServletResponse.SC_OK);
      } else {
        setResponseCode(response, HttpServletResponse.SC_OK);
      }
      response.setContentLength(brokerLoad.length());
      response.getOutputStream().write(brokerLoad.getBytes(StandardCharsets.UTF_8));
    } else if (granularity.toLowerCase().equals(GRANULARITY_REPLICA)) {
      ClusterModel clusterModel = _kafkaCruiseControl.clusterModel(time, requirements);
      if (json) {
        String data = clusterModel.getJSONString();
        setJSONResponseCode(request, response, HttpServletResponse.SC_OK);
        response.setContentLength(data.length());
        response.getOutputStream().write(data.getBytes(StandardCharsets.UTF_8));
      } else {
        setResponseCode(response, HttpServletResponse.SC_OK);
        // Write to stream to avoid expensive toString() call.
        clusterModel.writeTo(response.getOutputStream());
      }
    } else {
      throw new UserRequestException("Unknown granularity " + granularity);
    }
    response.getOutputStream().flush();
  }

  private void getPartitionLoad(HttpServletRequest request, HttpServletResponse response) throws Exception {
    Resource resource;
    Long startMs;
    Long endMs;
    boolean json;
    try {
      String resourceString = request.getParameter(RESOURCE_PARAM);
      json = wantJSON(request);
      try {
        resource = Resource.valueOf(resourceString.toUpperCase());
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
    OutputStream out = response.getOutputStream();
    if (!json) {
      int topicNameLength = clusterModel.topics().stream().mapToInt(String::length).max().orElse(20) + 5;
      setResponseCode(response, HttpServletResponse.SC_OK);
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
    } else {
      Map<String, Object> partitionMap = new HashMap<>();
      List<Object> partitionList = new ArrayList<>();
      List<String> header = new ArrayList<String>(Arrays.asList("topic", "partition", "leader", "followers", "CPU", "DISK", "NW_IN", "NW_OUT"));
      partitionMap.put("header", header);
      for (Partition p : sortedPartitions) {
        List<Integer> followers = p.followers().stream().map((replica) -> replica.broker().id()).collect(Collectors.toList());
        List<Object> record = new ArrayList<>();
        record.add(p.leader().topicPartition().topic());
        record.add(p.leader().topicPartition().partition());
        record.add(p.leader().broker().id());
        record.add(followers);
        record.add(p.leader().load().expectedUtilizationFor(Resource.CPU));
        record.add(p.leader().load().expectedUtilizationFor(Resource.DISK));
        record.add(p.leader().load().expectedUtilizationFor(Resource.NW_IN));
        record.add(p.leader().load().expectedUtilizationFor(Resource.NW_OUT));
        partitionList.add(record);
      }
      partitionMap.put("records", partitionList);
      Gson gson = new Gson();
      String g = gson.toJson(partitionMap);
      setJSONResponseCode(request, response, HttpServletResponse.SC_OK);
      response.setContentLength(g.length());
      out.write(g.getBytes(StandardCharsets.UTF_8));
    }
    out.flush();
  }

  private void getProposals(HttpServletRequest request, HttpServletResponse response) throws Exception {
    boolean verbose;
    boolean ignoreProposalCache;
    DataFrom dataFrom = DataFrom.VALID_WINDOWS;
    boolean json;
    List<String> goals;
    try {
      json = wantJSON(request);
      String verboseString = request.getParameter(VERBOSE_PARAM);
      verbose = verboseString != null && Boolean.parseBoolean(verboseString);
      String ignoreProposalCacheString = request.getParameter(IGNORE_PROPOSAL_CACHE_PARAM);
      String dataFromString = request.getParameter(DATA_FROM_PARAM);
      String goalsString = request.getParameter(GOALS_PARAM);
      goals = goalsString == null ? new ArrayList<>() : Arrays.asList(goalsString.split(","));
      goals.removeIf(String::isEmpty);
      ignoreProposalCache = (ignoreProposalCacheString != null && Boolean.parseBoolean(ignoreProposalCacheString))
          || !goals.isEmpty();
      if (dataFromString != null) {
        dataFrom = DataFrom.valueOf(dataFromString.toUpperCase());
      }
    } catch (Exception e) {
      throw new UserRequestException(e);
    }
    GoalsAndRequirements goalsAndRequirements = getGoalsAndRequirements(goals, dataFrom, ignoreProposalCache);

    GoalOptimizer.OptimizerResult optimizerResult =
        _kafkaCruiseControl.getOptimizationProposals(goalsAndRequirements.goals(),
                                                     goalsAndRequirements.requirements());
    String loadBeforeOptimization = json ?
      optimizerResult.brokerStatsBeforeOptimization().getJSONString() :
      optimizerResult.brokerStatsBeforeOptimization().toString();
    String loadAfterOptimization = json ?
      optimizerResult.brokerStatsAfterOptimization().getJSONString() :
      optimizerResult.brokerStatsAfterOptimization().toString();

    setResponseCode(response, HttpServletResponse.SC_OK);
    OutputStream out = response.getOutputStream();

    if (!json) {
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
      setResponseCode(response, HttpServletResponse.SC_OK);
      // Print summary before & after optimization
      out.write(String.format("%n%nCurrent load:").getBytes(StandardCharsets.UTF_8));
      out.write(loadBeforeOptimization.getBytes(StandardCharsets.UTF_8));
      out.write(String.format("%n%nOptimized load:").getBytes(StandardCharsets.UTF_8));
      out.write(loadAfterOptimization.getBytes(StandardCharsets.UTF_8));
    } else {
      if (!verbose) {
        out.write(KafkaCruiseControlServletUtils.getProposalSummary(optimizerResult).getBytes(StandardCharsets.UTF_8));
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
      Map<String, Object> proposalMap = new HashMap<>();
      proposalMap.put("goals", allGoals);
      proposalMap.put("loadBeforeOptimization", optimizerResult.brokerStatsBeforeOptimization().getJsonStructure());
      proposalMap.put("loadAfterOptimization", optimizerResult.brokerStatsAfterOptimization().getJsonStructure());
      Gson gson = new GsonBuilder()
                      .serializeNulls()
                      .serializeSpecialFloatingPointValues()
                      .create();
      String proposalsString = gson.toJson(proposalMap);
      setJSONResponseCode(request, response, HttpServletResponse.SC_OK);
      out.write(proposalsString.getBytes(StandardCharsets.UTF_8));
    }
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

  private ModelCompletenessRequirements getRequirements(DataFrom dataFrom) {
    if (dataFrom == DataFrom.VALID_PARTITIONS) {
      return new ModelCompletenessRequirements(Integer.MAX_VALUE, 0.0, true);
    } else {
      return new ModelCompletenessRequirements(1, 1.0, true);
    }
  }

  private boolean wantJSON(HttpServletRequest request) {
    boolean json;
    String jsonString = request.getParameter(JSON_PARAM);
    json = jsonString != null && Boolean.parseBoolean(jsonString);
    return json;
  }

  private void getState(HttpServletRequest request, HttpServletResponse response) throws Exception {
    boolean verbose;
    boolean superVerbose;
    boolean json;
    try {
      String verboseString = request.getParameter(VERBOSE_PARAM);
      verbose = verboseString != null && Boolean.parseBoolean(verboseString);
      String superVerboseString = request.getParameter(SUPER_VERBOSE_PARM);
      superVerbose = superVerboseString != null && Boolean.parseBoolean(superVerboseString);
      json = wantJSON(request);
    } catch (Exception e) {
      throw new UserRequestException(e);
    }
    KafkaCruiseControlState state = _kafkaCruiseControl.state();
    OutputStream out = response.getOutputStream();
    if (json) {
      String stateString = state.getJSONString();
      setJSONResponseCode(request, response, HttpServletResponse.SC_OK);
      response.setContentLength(stateString.length());
      out.write(stateString.getBytes(StandardCharsets.UTF_8));
    } else {
      String stateString = state.toString();
      setResponseCode(response, HttpServletResponse.SC_OK);
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
        }
      }
    }
    response.getOutputStream().flush();
  }

  private void addOrRemoveBroker(HttpServletRequest request, HttpServletResponse response, EndPoint endPoint)
      throws KafkaCruiseControlException, IOException {
    List<Integer> brokerIds = new ArrayList<>();
    boolean dryrun;
    DataFrom dataFrom = DataFrom.VALID_WINDOWS; // by default we use ValidWindows
    boolean throttleAddedOrRemovedBrokers;
    List<String> goals;
    boolean json;
    try {
      // TODO: Handle urlencoded value (%2C instead of ,)
      String[] brokerIdsString = request.getParameter(BROKER_ID_PARAM).split(",");
      for (String brokerIdString : brokerIdsString) {
        brokerIds.add(Integer.parseInt(brokerIdString));
      }
      String dryrunString = request.getParameter(DRY_RUN_PARAM);
      dryrun = dryrunString == null || Boolean.parseBoolean(dryrunString);
      String throttleBrokerString = endPoint == EndPoint.ADD_BROKER ?
          request.getParameter(THROTTLE_ADDED_BROKER_PARAM) : request.getParameter(THROTTLE_REMOVED_BROKER_PARAM);
      throttleAddedOrRemovedBrokers = throttleBrokerString == null || Boolean.parseBoolean(throttleBrokerString);
      String goalsString = request.getParameter(GOALS_PARAM);
      // TODO: Handle urlencoded value (%2C instead of ,)
      goals = goalsString == null || goalsString.isEmpty() ? Collections.emptyList() : Arrays.asList(goalsString.split(","));
      goals.removeIf(String::isEmpty);
      String dataFromString = request.getParameter(DATA_FROM_PARAM);
      if (dataFromString != null) {
        dataFrom = DataFrom.valueOf(dataFromString);
      }
    } catch (Exception e) {
      throw new UserRequestException(e);
    }
    GoalsAndRequirements goalsAndRequirements = getGoalsAndRequirements(goals, dataFrom, false);
    GoalOptimizer.OptimizerResult optimizerResult;
    if (endPoint == EndPoint.ADD_BROKER) {
      optimizerResult = _kafkaCruiseControl.addBrokers(brokerIds, dryrun, throttleAddedOrRemovedBrokers,
                                                       goalsAndRequirements.goals(),
                                                       goalsAndRequirements.requirements());
    } else {
      optimizerResult = _kafkaCruiseControl.decommissionBrokers(brokerIds, dryrun, throttleAddedOrRemovedBrokers,
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
    DataFrom dataFrom = DataFrom.VALID_WINDOWS; // default to with available windows.
    List<String> goals;
    try {
      String dryrunString = request.getParameter(DRY_RUN_PARAM);
      dryrun = dryrunString == null || Boolean.parseBoolean(dryrunString);

      String goalsString = request.getParameter(GOALS_PARAM);
      goals = goalsString == null ? new ArrayList<>() : Arrays.asList(goalsString.split(","));
      goals.removeIf(String::isEmpty);

      String dataFromString = request.getParameter(DATA_FROM_PARAM);
      if (dataFromString != null) {
        dataFrom = DataFrom.valueOf(dataFromString);
      }
    } catch (Exception e) {
      throw new UserRequestException(e);
    }
    GoalsAndRequirements goalsAndRequirements = getGoalsAndRequirements(goals, dataFrom, false);
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
    response.setStatus(code);
    response.setContentType("text/plain");
    response.setCharacterEncoding("utf-8");
    response.setHeader("Access-Control-Allow-Origin", "*");
    response.setHeader("Access-Control-Request-Method", "OPTIONS, GET, POST");
  }

  private void setJSONResponseCode(HttpServletRequest request, HttpServletResponse response, int code) {
    response.setStatus(code);
    response.setContentType("application/json");
    response.setCharacterEncoding("utf-8");
    response.setHeader("Access-Control-Allow-Origin", "*");
    response.setHeader("Access-Control-Request-Method", "OPTIONS, GET, POST");
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
  
  // package private for testing.
  GoalsAndRequirements getGoalsAndRequirements(List<String> userProvidedGoals,
                                               DataFrom dataFrom,
                                               boolean ignoreCache) {
    if (!userProvidedGoals.isEmpty() || dataFrom == DataFrom.VALID_PARTITIONS) {
      return new GoalsAndRequirements(userProvidedGoals, getRequirements(dataFrom));
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
      return new GoalsAndRequirements(ignoreCache ? allGoals : Collections.emptyList(), 
                                      getRequirements(DataFrom.VALID_WINDOWS));
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
