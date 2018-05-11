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
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerEvenRackAwareGoal;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Partition;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.SampleExtrapolation;
import com.linkedin.kafka.cruisecontrol.async.AsyncKafkaCruiseControl;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
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
import java.util.stream.Collectors;
import javax.servlet.ServletOutputStream;
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
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServlet.EndPoint.*;
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

  private static final String JSON_PARAM = "json";
  private static final String START_MS_PARAM = "start";
  private static final String END_MS_PARAM = "end";
  private static final String ENTRIES = "entries";
  private static final String CLEAR_METRICS_PARAM = "clearmetrics";
  private static final String TIME_PARAM = "time";
  private static final String VERBOSE_PARAM = "verbose";
  private static final String SUPER_VERBOSE_PARM = "super_verbose";
  private static final String RESOURCE_PARAM = "resource";
  private static final String DATA_FROM_PARAM = "data_from";
  private static final String KAFKA_ASSIGNER_MODE_PARAM = "kafka_assigner";

  private static final String GOALS_PARAM = "goals";
  private static final String GRANULARITY_PARAM = "granularity";
  private static final String GRANULARITY_BROKER = "broker";

  private static final String GRANULARITY_REPLICA = "replica";
  private static final String BROKER_ID_PARAM = "brokerid";
  private static final String DRY_RUN_PARAM = "dryrun";
  private static final String THROTTLE_ADDED_BROKER_PARAM = "throttle_added_broker";
  private static final String THROTTLE_REMOVED_BROKER_PARAM = "throttle_removed_broker";
  private static final String IGNORE_PROPOSAL_CACHE_PARAM = "ignore_proposal_cache";
  private static final String DEFAULT_PARTITION_LOAD_RESOURCE = "disk";
  private static final int JSON_VERSION = 1;

  private static final String PARTITION_MOVEMENTS = "partition movements";
  private static final String LEADERSHIP_MOVEMENTS = "leadership movements";

  private static final Map<EndPoint, Set<String>> VALID_ENDPOINT_PARAM_NAMES;
  static {
    Map<EndPoint, Set<String>> validParamNames = new HashMap<>();

    Set<String> bootstrap = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    bootstrap.add(START_MS_PARAM);
    bootstrap.add(END_MS_PARAM);
    bootstrap.add(CLEAR_METRICS_PARAM);
    bootstrap.add(JSON_PARAM);

    Set<String> train = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    train.add(START_MS_PARAM);
    train.add(END_MS_PARAM);
    train.add(JSON_PARAM);

    Set<String> load = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    load.add(TIME_PARAM);
    load.add(GRANULARITY_PARAM);
    load.add(JSON_PARAM);

    Set<String> partitionLoad = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    partitionLoad.add(RESOURCE_PARAM);
    partitionLoad.add(START_MS_PARAM);
    partitionLoad.add(END_MS_PARAM);
    partitionLoad.add(ENTRIES);
    partitionLoad.add(JSON_PARAM);

    Set<String> proposals = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    proposals.add(VERBOSE_PARAM);
    proposals.add(IGNORE_PROPOSAL_CACHE_PARAM);
    proposals.add(DATA_FROM_PARAM);
    proposals.add(GOALS_PARAM);
    proposals.add(KAFKA_ASSIGNER_MODE_PARAM);
    proposals.add(JSON_PARAM);

    Set<String> state = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    state.add(VERBOSE_PARAM);
    state.add(SUPER_VERBOSE_PARM);
    state.add(JSON_PARAM);

    Set<String> addOrRemoveBroker = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    addOrRemoveBroker.add(BROKER_ID_PARAM);
    addOrRemoveBroker.add(DRY_RUN_PARAM);
    addOrRemoveBroker.add(THROTTLE_REMOVED_BROKER_PARAM);
    addOrRemoveBroker.add(DATA_FROM_PARAM);
    addOrRemoveBroker.add(GOALS_PARAM);
    addOrRemoveBroker.add(KAFKA_ASSIGNER_MODE_PARAM);
    addOrRemoveBroker.add(JSON_PARAM);

    Set<String> addBroker = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    addBroker.add(THROTTLE_ADDED_BROKER_PARAM);
    addBroker.addAll(addOrRemoveBroker);

    Set<String> removeBroker = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    removeBroker.add(THROTTLE_REMOVED_BROKER_PARAM);
    removeBroker.addAll(addOrRemoveBroker);

    Set<String> demoteBroker = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    demoteBroker.add(BROKER_ID_PARAM);
    demoteBroker.add(DRY_RUN_PARAM);
    addOrRemoveBroker.add(JSON_PARAM);

    Set<String> rebalance = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    rebalance.add(DRY_RUN_PARAM);
    rebalance.add(GOALS_PARAM);
    rebalance.add(KAFKA_ASSIGNER_MODE_PARAM);
    rebalance.add(DATA_FROM_PARAM);
    rebalance.add(JSON_PARAM);

    Set<String> kafkaClusterState = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    kafkaClusterState.add(VERBOSE_PARAM);
    kafkaClusterState.add(JSON_PARAM);

    validParamNames.put(BOOTSTRAP, Collections.unmodifiableSet(bootstrap));
    validParamNames.put(TRAIN, Collections.unmodifiableSet(train));
    validParamNames.put(LOAD, Collections.unmodifiableSet(load));
    validParamNames.put(PARTITION_LOAD, Collections.unmodifiableSet(partitionLoad));
    validParamNames.put(PROPOSALS, Collections.unmodifiableSet(proposals));
    validParamNames.put(STATE, Collections.unmodifiableSet(state));
    validParamNames.put(ADD_BROKER, Collections.unmodifiableSet(addBroker));
    validParamNames.put(REMOVE_BROKER, Collections.unmodifiableSet(removeBroker));
    validParamNames.put(DEMOTE_BROKER, Collections.unmodifiableSet(demoteBroker));
    validParamNames.put(REBALANCE, Collections.unmodifiableSet(rebalance));
    validParamNames.put(STOP_PROPOSAL_EXECUTION, Collections.emptySet());
    validParamNames.put(PAUSE_SAMPLING, Collections.emptySet());
    validParamNames.put(RESUME_SAMPLING, Collections.emptySet());
    validParamNames.put(KAFKA_CLUSTER_STATE, Collections.unmodifiableSet(kafkaClusterState));

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
   *    GET /kafkacruisecontrol/load?time=[TIMESTAMP]&amp;granularity=[GRANULARITY]
   *    The valid granularity value are "replica" and "broker". The default is broker level.
   *
   * 4. Get the partition load sorted by the utilization of a given resource
   *    GET /kafkacruisecontrol/partition_load?resource=[RESOURCE]&amp;start=[START_TIMESTAMP]&amp;end=[END_TIMESTAMP]
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
    ACCESS_LOG.info("Received {}, {} from {}", urlEncode(request.toString()), urlEncode(request.getRequestURL().toString()),
                    KafkaCruiseControlServletUtils.getClientIpAddress(request));
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
        setErrorResponse(response, "", errorMessage, SC_NOT_FOUND, wantJSON(request));
      }
    } catch (UserRequestException ure) {
      LOG.error("Why are you failing?", ure);
      String errorMessage = String.format("Bad GET request '%s'", request.getPathInfo());
      StringWriter sw = new StringWriter();
      ure.printStackTrace(new PrintWriter(sw));
      setErrorResponse(response, sw.toString(), errorMessage, SC_BAD_REQUEST, wantJSON(request));
      _sessionManager.closeSession(request);
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);
      String errorMessage = String.format("Error processing GET request '%s' due to '%s'.", request.getPathInfo(), e.getMessage());
      LOG.error(errorMessage, e);
      setErrorResponse(response, sw.toString(), errorMessage, SC_INTERNAL_SERVER_ERROR, wantJSON(request));
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
   *
   * 2. Add a broker
   *    POST /kafkacruisecontrol/add_broker?brokerid=[id1,id2...]&amp;dryrun=[true/false]&amp;throttle_added_broker=[true/false]&amp;goals=[goal1,goal2...]
   *
   * 3. Trigger a workload balance.
   *    POST /kafkacruisecontrol/rebalance?dryrun=[true/false]&amp;force=[true/false]&amp;goals=[goal1,goal2...]
   *
   * 4. Stop the proposal execution.
   *    POST /kafkacruisecontrol/stop_proposal_execution
   *
   * 5.Pause metrics sampling. (RUNNING -&gt; PAUSED).
   *    POST /kafkacruisecontrol/pause_sampling
   *
   * 6.Resume metrics sampling. (PAUSED -&gt; RUNNING).
   *    POST /kafkacruisecontrol/resume_sampling
   *
   *
   * <b>NOTE: All the timestamps are epoch time in second granularity.</b>
   * </pre>
   */
  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
    ACCESS_LOG.info("Received {}, {} from {}", urlEncode(request.toString()), urlEncode(request.getRequestURL().toString()),
                    KafkaCruiseControlServletUtils.getClientIpAddress(request));
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
          switch (endPoint) {
            case ADD_BROKER:
            case REMOVE_BROKER:
              if (addOrRemoveBroker(request, response, endPoint)) {
                _sessionManager.closeSession(request);
              }
              break;
            case REBALANCE:
              if (rebalance(request, response)) {
                _sessionManager.closeSession(request);
              }
              break;
            case STOP_PROPOSAL_EXECUTION:
              stopProposalExecution();
              break;
            case PAUSE_SAMPLING:
              pauseSampling();
              break;
            case RESUME_SAMPLING:
              resumeSampling();
              break;
            case DEMOTE_BROKER:
              if (demoteBroker(request, response)) {
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
        setErrorResponse(response, "", errorMessage, SC_NOT_FOUND, wantJSON(request));
      }
    } catch (UserRequestException ure) {
      LOG.error("Why are you failing?", ure);
      String errorMessage = String.format("Bad POST request '%s'", request.getPathInfo());
      StringWriter sw = new StringWriter();
      ure.printStackTrace(new PrintWriter(sw));
      setErrorResponse(response, sw.toString(), errorMessage, SC_BAD_REQUEST, wantJSON(request));
      _sessionManager.closeSession(request);
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);
      String errorMessage = String.format("Error processing POST request '%s' due to: '%s'.", request.getPathInfo(), e.getMessage());
      LOG.error(errorMessage, e);
      setErrorResponse(response, sw.toString(), errorMessage, SC_INTERNAL_SERVER_ERROR, wantJSON(request));
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
    boolean json = wantJSON(request);
    try {
      String startMsString = request.getParameter(START_MS_PARAM);
      String endMsString = request.getParameter(END_MS_PARAM);
      String clearMetricsString = request.getParameter(CLEAR_METRICS_PARAM);
      startMs = startMsString == null ? null : Long.parseLong(startMsString);
      endMs = endMsString == null ? null : Long.parseLong(endMsString);
      clearMetrics = clearMetricsString == null || Boolean.parseBoolean(clearMetricsString);
      if (startMs == null && endMs != null) {
        String errorMsg = "The start time cannot be empty when end time is specified.";
        setErrorResponse(response, "", errorMsg, SC_BAD_REQUEST, json);
        return;
      }
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      setErrorResponse(response, sw.toString(), e.getMessage(), SC_BAD_REQUEST, json);
      return;
    }

    if (startMs != null && endMs != null) {
      _asyncKafkaCruiseControl.bootstrapLoadMonitor(startMs, endMs, clearMetrics);
    } else if (startMs != null) {
      _asyncKafkaCruiseControl.bootstrapLoadMonitor(startMs, clearMetrics);
    } else {
      _asyncKafkaCruiseControl.bootstrapLoadMonitor(clearMetrics);
    }

    String errorMsg = String.format("Bootstrap started. Check status through %s", getStateCheckUrl(request));
    setErrorResponse(response, "", errorMsg, SC_OK, json);
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
      setJSONResponseCode(response, responseCode);
    } else {
      resp = errorMessage == null ? "" : errorMessage;
      setResponseCode(response, responseCode);
    }
    response.setContentLength(resp.length());
    response.getOutputStream().write(resp.getBytes(StandardCharsets.UTF_8));
    response.getOutputStream().flush();
  }

  private void train(HttpServletRequest request, HttpServletResponse response) throws Exception {
    Long startMs;
    Long endMs;
    boolean json = wantJSON(request);
    try {
      String startMsString = request.getParameter(START_MS_PARAM);
      String endMsString = request.getParameter(END_MS_PARAM);
      startMs = Long.parseLong(startMsString);
      endMs = Long.parseLong(endMsString);
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      setErrorResponse(response, sw.toString(), e.getMessage(), SC_BAD_REQUEST, json);
      return;
    }
    _asyncKafkaCruiseControl.trainLoadModel(startMs, endMs);
    String errorMsg = String.format("Load model training started. Check status through %s", getStateCheckUrl(request));
    setErrorResponse(response, "", errorMsg, SC_OK, json);
  }

  private boolean getClusterLoad(HttpServletRequest request, HttpServletResponse response) throws Exception {
    long time;
    String granularity;
    boolean json = wantJSON(request);
    try {
      String timeString = request.getParameter(TIME_PARAM);
      time = (timeString == null || timeString.toUpperCase().equals("NOW"))
          ? System.currentTimeMillis() : Long.parseLong(timeString);
      granularity = request.getParameter(GRANULARITY_PARAM);
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      setErrorResponse(response, sw.toString(), e.getMessage(), SC_BAD_REQUEST, json);
      // Close session
      return true;
    }

    ModelCompletenessRequirements requirements = new ModelCompletenessRequirements(1, 0.0, true);
    if (granularity == null || granularity.toLowerCase().equals(GRANULARITY_BROKER)) {
      ClusterModel.BrokerStats brokerStats = _asyncKafkaCruiseControl.cachedBrokerLoadStats();
      String brokerLoad;
      if (brokerStats != null) {
        if (json) {
          brokerLoad = brokerStats.getJSONString(JSON_VERSION);
        } else {
          brokerLoad = brokerStats.toString();
        }
      } else {
        // Get the broker stats asynchronously.
        brokerStats = getAndMaybeReturnProgress(request, response,
                                                () -> _asyncKafkaCruiseControl.getBrokerStats(time, requirements));
        if (brokerStats == null) {
          return false;
        }
        if (json) {
          brokerLoad = brokerStats.getJSONString(JSON_VERSION);
        } else {
          brokerLoad = brokerStats.toString();
        }
      }
      if (json) {
        setJSONResponseCode(response, SC_OK);
      } else {
        setResponseCode(response, SC_OK);
      }
      response.setContentLength(brokerLoad.length());
      response.getOutputStream().write(brokerLoad.getBytes(StandardCharsets.UTF_8));
    } else if (granularity.toLowerCase().equals(GRANULARITY_REPLICA)) {
      // Get the cluster model asynchronously
      ClusterModel clusterModel = getAndMaybeReturnProgress(request, response,
                                                            () -> _asyncKafkaCruiseControl.clusterModel(time, requirements));
      if (clusterModel == null) {
        return false;
      }
      if (json) {
        String data = clusterModel.getJSONString(JSON_VERSION);
        setJSONResponseCode(response, SC_OK);
        response.setContentLength(data.length());
        ServletOutputStream os = response.getOutputStream();
        OutputStreamWriter writer = new OutputStreamWriter(os, StandardCharsets.UTF_8);
        writer.write(data);
        writer.flush();
      } else {
        setResponseCode(response, SC_OK);
        // Write to stream to avoid expensive toString() call.
        clusterModel.writeTo(response.getOutputStream());
      }
    } else {
      String errorMsg = String.format("Unknown granularity %s", granularity);
      setErrorResponse(response, "", errorMsg, SC_BAD_REQUEST, json);
      // Close session
      return true;
    }
    response.getOutputStream().flush();
    return true;
  }

  private boolean getPartitionLoad(HttpServletRequest request, HttpServletResponse response) throws Exception {
    Resource resource;
    Long startMs;
    Long endMs;
    boolean json = wantJSON(request);
    try {
      String resourceString = request.getParameter(RESOURCE_PARAM);
      try {
        if (resourceString == null) {
          resourceString = DEFAULT_PARTITION_LOAD_RESOURCE;
        }
        resource = Resource.valueOf(resourceString.toUpperCase());
      } catch (IllegalArgumentException iae) {
        String errorMsg = String.format("Invalid resource type %s. The resource type must be one of the following: "
                                        + "CPU, DISK, NW_IN, NW_OUT", resourceString);
        StringWriter sw = new StringWriter();
        iae.printStackTrace(new PrintWriter(sw));
        setErrorResponse(response, sw.toString(), errorMsg, SC_BAD_REQUEST, json);
        // Close session
        return true;
      }
      String startMsString = request.getParameter(START_MS_PARAM);
      String endMsString = request.getParameter(END_MS_PARAM);
      startMs = startMsString == null ? -1L : Long.parseLong(startMsString);
      endMs = endMsString == null ? System.currentTimeMillis() : Long.parseLong(endMsString);
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      setErrorResponse(response, sw.toString(), e.getMessage(), SC_BAD_REQUEST, json);
      // Close session
      return true;
    }

    ModelCompletenessRequirements requirements = new ModelCompletenessRequirements(1, 0.98, false);
    // Get cluster model asynchronously.
    ClusterModel clusterModel =
        getAndMaybeReturnProgress(request, response,
                                  () -> _asyncKafkaCruiseControl.clusterModel(startMs, endMs, requirements));
    if (clusterModel == null) {
      return false;
    }
    List<Partition> sortedPartitions = clusterModel.replicasSortedByUtilization(resource);
    OutputStream out = response.getOutputStream();

    String entriesString = request.getParameter(ENTRIES);
    Integer entries = entriesString == null ? Integer.MAX_VALUE : Integer.parseInt(entriesString);
    int numEntries = 0;
    if (!json) {
      int topicNameLength = clusterModel.topics().stream().mapToInt(String::length).max().orElse(20) + 5;
      setResponseCode(response, SC_OK);
      out.write(String.format("%" + topicNameLength + "s%10s%30s%20s%20s%20s%20s%n", "PARTITION", "LEADER", "FOLLOWERS",
                              "CPU (%)", "DISK (MB)", "NW_IN (KB/s)", "NW_OUT (KB/s)")
                      .getBytes(StandardCharsets.UTF_8));
      for (Partition p : sortedPartitions) {
        if (++numEntries > entries) {
          break;
        }
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
      List<String> header = new ArrayList<>(Arrays.asList("topic", "partition", "leader", "followers", "CPU", "DISK", "NW_IN", "NW_OUT"));
      partitionMap.put("version", JSON_VERSION);
      partitionMap.put("header", header);
      for (Partition p : sortedPartitions) {
        if (++numEntries > entries) {
          break;
        }
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
      setJSONResponseCode(response, SC_OK);
      response.setContentLength(g.length());
      out.write(g.getBytes(StandardCharsets.UTF_8));
    }
    out.flush();
    return true;
  }

  private boolean getProposals(HttpServletRequest request, HttpServletResponse response) throws Exception {
    boolean verbose;
    boolean ignoreProposalCache;
    DataFrom dataFrom;
    List<String> goals;
    boolean json = wantJSON(request);
    try {
      String verboseString = request.getParameter(VERBOSE_PARAM);
      verbose = Boolean.parseBoolean(verboseString);

      goals = getGoals(request);
      dataFrom = getDataFrom(request);

      String ignoreProposalCacheString = request.getParameter(IGNORE_PROPOSAL_CACHE_PARAM);
      ignoreProposalCache = (Boolean.parseBoolean(ignoreProposalCacheString))
          || !goals.isEmpty();
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      setErrorResponse(response, sw.toString(), e.getMessage(), SC_BAD_REQUEST, json);
      // Close session
      return true;
    }
    GoalsAndRequirements goalsAndRequirements =
        getGoalsAndRequirements(request, response, goals, dataFrom, ignoreProposalCache);
    if (goalsAndRequirements == null) {
      return false;
    }
    // Get the optimization result asynchronously.
    GoalOptimizer.OptimizerResult optimizerResult =
        getAndMaybeReturnProgress(request, response,
                                  () -> _asyncKafkaCruiseControl.getOptimizationProposals(goalsAndRequirements.goals(),
                                                                                          goalsAndRequirements.requirements()));
    if (optimizerResult == null) {
      return false;
    }

    setResponseCode(response, SC_OK);
    OutputStream out = response.getOutputStream();

    if (!json) {
      String loadBeforeOptimization = optimizerResult.brokerStatsBeforeOptimization().toString();
      String loadAfterOptimization = optimizerResult.brokerStatsAfterOptimization().toString();
      if (!verbose) {
        out.write(
            KafkaCruiseControlServletUtils.getProposalSummary(optimizerResult).getBytes(StandardCharsets.UTF_8));
      } else {
        out.write(optimizerResult.goalProposals().toString().getBytes(StandardCharsets.UTF_8));
      }
      for (Map.Entry<Goal, ClusterModelStats> entry : optimizerResult.statsByGoalPriority().entrySet()) {
        Goal goal = entry.getKey();
        out.write(String.format("%n%nStats for goal %s%s:%n", goal.name(), goalResultDescription(goal, optimizerResult))
            .getBytes(StandardCharsets.UTF_8));
        out.write(entry.getValue().toString().getBytes(StandardCharsets.UTF_8));
      }
      setResponseCode(response, SC_OK);
      // Print summary before & after optimization
      out.write(String.format("%n%nCurrent load:").getBytes(StandardCharsets.UTF_8));
      out.write(loadBeforeOptimization.getBytes(StandardCharsets.UTF_8));
      out.write(String.format("%n%nOptimized load:").getBytes(StandardCharsets.UTF_8));
      out.write(loadAfterOptimization.getBytes(StandardCharsets.UTF_8));
    } else {
      if (!verbose) {
        out.write(
            KafkaCruiseControlServletUtils.getProposalSummary(optimizerResult).getBytes(StandardCharsets.UTF_8));
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
      proposalMap.put("version", JSON_VERSION);
      proposalMap.put("goals", allGoals);
      proposalMap.put("loadBeforeOptimization", optimizerResult.brokerStatsBeforeOptimization().getJsonStructure());
      proposalMap.put("loadAfterOptimization", optimizerResult.brokerStatsAfterOptimization().getJsonStructure());
      Gson gson = new GsonBuilder()
                      .serializeNulls()
                      .serializeSpecialFloatingPointValues()
                      .create();
      String proposalsString = gson.toJson(proposalMap);
      setJSONResponseCode(response, SC_OK);
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

  private boolean wantJSON(HttpServletRequest request) {
    String jsonString = request.getParameter(JSON_PARAM);
    return Boolean.parseBoolean(jsonString);
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
    boolean verbose;
    boolean json = wantJSON(request);
    try {
      String verboseString = request.getParameter(VERBOSE_PARAM);
      verbose = Boolean.parseBoolean(verboseString);
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      setErrorResponse(response, sw.toString(), e.getMessage(), SC_BAD_REQUEST, json);
      return;
    }
    KafkaClusterState state = _asyncKafkaCruiseControl.kafkaClusterState();
    OutputStream out = response.getOutputStream();
    if (json) {
      String stateString = state.getJSONString(JSON_VERSION, verbose);
      setJSONResponseCode(response, SC_OK);
      response.setContentLength(stateString.length());
      out.write(stateString.getBytes(StandardCharsets.UTF_8));
    } else {
      setResponseCode(response, SC_OK);
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
    boolean verbose;
    boolean superVerbose;
    boolean json = wantJSON(request);
    try {
      String verboseString = request.getParameter(VERBOSE_PARAM);
      verbose = Boolean.parseBoolean(verboseString);
      String superVerboseString = request.getParameter(SUPER_VERBOSE_PARM);
      superVerbose = Boolean.parseBoolean(superVerboseString);
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      setErrorResponse(response, sw.toString(), e.getMessage(), SC_BAD_REQUEST, json);
      // Close session
      return true;
    }
    KafkaCruiseControlState state = getAndMaybeReturnProgress(request, response, _asyncKafkaCruiseControl::state);
    if (state == null) {
      return false;
    }
    OutputStream out = response.getOutputStream();
    if (json) {
      String stateString = state.getJSONString(JSON_VERSION);
      setJSONResponseCode(response, SC_OK);
      response.setContentLength(stateString.length());
      out.write(stateString.getBytes(StandardCharsets.UTF_8));
    } else {
      String stateString = state.toString();
      setResponseCode(response, SC_OK);
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
          out.write(String.format("%n%nLinear Regression Model State:%n%s", state.monitorState().detailTrainingProgress())
                        .getBytes(StandardCharsets.UTF_8));
        }
      }
    }
    response.getOutputStream().flush();
    return true;
  }

  private boolean addOrRemoveBroker(HttpServletRequest request, HttpServletResponse response, EndPoint endPoint)
      throws Exception {
    List<Integer> brokerIds = new ArrayList<>();
    boolean dryrun;
    DataFrom dataFrom;
    boolean throttleAddedOrRemovedBrokers;
    List<String> goals;
    boolean json = wantJSON(request);
    try {
      String[] brokerIdsString = request.getParameter(BROKER_ID_PARAM).split(",");
      for (String brokerIdString : brokerIdsString) {
        brokerIds.add(Integer.parseInt(brokerIdString));
      }

      dryrun = getDryRun(request);
      goals = getGoals(request);
      dataFrom = getDataFrom(request);

      String throttleBrokerString = endPoint == ADD_BROKER ? request.getParameter(THROTTLE_ADDED_BROKER_PARAM)
                                                           : request.getParameter(THROTTLE_REMOVED_BROKER_PARAM);
      throttleAddedOrRemovedBrokers = throttleBrokerString == null || Boolean.parseBoolean(throttleBrokerString);

    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      setErrorResponse(response, sw.toString(), e.getMessage(), SC_BAD_REQUEST, json);
      // Close session
      return true;
    }
    GoalsAndRequirements goalsAndRequirements = getGoalsAndRequirements(request, response, goals, dataFrom, false);
    if (goalsAndRequirements == null) {
      return false;
    }
    // Get proposals asynchronously.
    GoalOptimizer.OptimizerResult optimizerResult;
    if (endPoint == ADD_BROKER) {
      optimizerResult =
          getAndMaybeReturnProgress(request, response,
                                    () -> _asyncKafkaCruiseControl.addBrokers(brokerIds,
                                                                              dryrun,
                                                                              throttleAddedOrRemovedBrokers,
                                                                              goalsAndRequirements.goals(),
                                                                              goalsAndRequirements.requirements()));
    } else {
      optimizerResult =
          getAndMaybeReturnProgress(request, response,
                                    () -> _asyncKafkaCruiseControl.decommissionBrokers(brokerIds,
                                                                                       dryrun,
                                                                                       throttleAddedOrRemovedBrokers,
                                                                                       goalsAndRequirements.goals(),
                                                                                       goalsAndRequirements.requirements()));
    }
    if (optimizerResult == null) {
      return false;
    }

    setResponseCode(response, SC_OK);
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
                            endPoint == ADD_BROKER ? "adding" : "removing", brokerIds)
                    .getBytes(StandardCharsets.UTF_8));
    out.write(optimizerResult.brokerStatsAfterOptimization().toString()
                             .getBytes(StandardCharsets.UTF_8));
    out.flush();
    return true;
  }

  private boolean rebalance(HttpServletRequest request, HttpServletResponse response) throws Exception {
    boolean dryrun;
    DataFrom dataFrom;
    List<String> goals;
    boolean json = wantJSON(request);
    try {
      dryrun = getDryRun(request);
      goals = getGoals(request);
      dataFrom = getDataFrom(request);
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      setErrorResponse(response, sw.toString(), e.getMessage(), SC_BAD_REQUEST, json);
      // Close session
      return true;
    }
    GoalsAndRequirements goalsAndRequirements = getGoalsAndRequirements(request, response, goals, dataFrom, false);
    if (goalsAndRequirements == null) {
      return false;
    }
    GoalOptimizer.OptimizerResult optimizerResult =
        getAndMaybeReturnProgress(request, response,
                                  () -> _asyncKafkaCruiseControl.rebalance(goalsAndRequirements.goals(),
                                                                           dryrun,
                                                                           goalsAndRequirements.requirements()));
    if (optimizerResult == null) {
      return false;
    }

    setResponseCode(response, SC_OK);
    OutputStream out = response.getOutputStream();
    out.write(
        KafkaCruiseControlServletUtils.getProposalSummary(optimizerResult).getBytes(StandardCharsets.UTF_8));
    for (Map.Entry<Goal, ClusterModelStats> entry : optimizerResult.statsByGoalPriority().entrySet()) {
      Goal goal = entry.getKey();
      out.write(String.format("%n%nStats for goal %s%s:%n", goal.name(), goalResultDescription(goal, optimizerResult))
                      .getBytes(StandardCharsets.UTF_8));
      out.write(entry.getValue().toString().getBytes(StandardCharsets.UTF_8));
    }
    out.write(String.format("%nCluster load after rebalance:%n").getBytes(StandardCharsets.UTF_8));
    out.write(optimizerResult.brokerStatsAfterOptimization().toString().getBytes(StandardCharsets.UTF_8));
    out.flush();
    return true;
  }

  private boolean demoteBroker(HttpServletRequest request, HttpServletResponse response) throws Exception {
    List<Integer> brokerIds = new ArrayList<>();
    boolean dryrun;
    boolean json = wantJSON(request);
    try {
      String[] brokerIdsString = request.getParameter(BROKER_ID_PARAM).split(",");
      for (String brokerIdString : brokerIdsString) {
        brokerIds.add(Integer.parseInt(brokerIdString));
      }
      dryrun = getDryRun(request);
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      setErrorResponse(response, sw.toString(), e.getMessage(), SC_BAD_REQUEST, json);
      // Close session
      return true;
    }

    // Get proposals asynchronously.
    GoalOptimizer.OptimizerResult optimizerResult =
          getAndMaybeReturnProgress(request, response, () -> _asyncKafkaCruiseControl.demoteBrokers(brokerIds, dryrun));
    if (optimizerResult == null) {
      return false;
    }

    setResponseCode(response, SC_OK);
    OutputStream out = response.getOutputStream();
    out.write(KafkaCruiseControlServletUtils.getProposalSummary(optimizerResult)
                                            .getBytes(StandardCharsets.UTF_8));
    for (Map.Entry<Goal, ClusterModelStats> entry : optimizerResult.statsByGoalPriority().entrySet()) {
      Goal goal = entry.getKey();
      out.write(String.format("%n%nStats for goal %s%s:%n", goal.name(), goalResultDescription(goal, optimizerResult))
                      .getBytes(StandardCharsets.UTF_8));
      out.write(entry.getValue().toString().getBytes(StandardCharsets.UTF_8));
    }
    out.write(String.format("%nCluster load after demote broker %s:%n", brokerIds)
                    .getBytes(StandardCharsets.UTF_8));
    out.write(optimizerResult.brokerStatsAfterOptimization().toString()
                             .getBytes(StandardCharsets.UTF_8));
    out.flush();
    return true;
  }

  private boolean getDryRun(HttpServletRequest request) {
    String dryrunString = request.getParameter(DRY_RUN_PARAM);
    return dryrunString == null || Boolean.parseBoolean(dryrunString);
  }

  private List<String> getGoals(HttpServletRequest request) {
    List<String> goals;
    boolean kafkaAssignerMode;
    String goalsString = request.getParameter(GOALS_PARAM);
    // TODO: Handle urlencoded value (%2C instead of ,)
    goals = goalsString == null ? new ArrayList<>() : Arrays.asList(goalsString.split(","));
    goals.removeIf(String::isEmpty);

    // If KafkaAssigner mode is enabled (default) and goals are not specified, it overrides the goal list.
    String kafkaAssignerModeString = request.getParameter(KAFKA_ASSIGNER_MODE_PARAM);
    kafkaAssignerMode = kafkaAssignerModeString != null && Boolean.parseBoolean(kafkaAssignerModeString);
    if (goals.isEmpty() && kafkaAssignerMode) {
      goals = Arrays.asList(KafkaAssignerEvenRackAwareGoal.class.getSimpleName(),
                            KafkaAssignerDiskUsageDistributionGoal.class.getSimpleName());
    }
    return goals;
  }

  private DataFrom getDataFrom(HttpServletRequest request) {
    DataFrom dataFrom = VALID_WINDOWS; // default to with available windows.
    String dataFromString = request.getParameter(DATA_FROM_PARAM);
    if (dataFromString != null) {
      dataFrom = DataFrom.valueOf(dataFromString.toUpperCase());
    }
    return dataFrom;
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
      returnProgress(response, future);
      return null;
    }
  }

  private EndPoint endPoint(HttpServletRequest request) {
    List<EndPoint> supportedEndpoints;
    switch (request.getMethod()) {
      case "GET":
        supportedEndpoints = EndPoint.getEndpoint();
        break;
      case "POST":
        supportedEndpoints = EndPoint.postEndpoint();
        break;
      default:
        throw new IllegalArgumentException("Unsupported request method: " + request.getMethod() + ".");
    }

    String path = request.getRequestURI().toUpperCase().replace("/KAFKACRUISECONTROL/", "");
    for (EndPoint endPoint : supportedEndpoints) {
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

  private void setJSONResponseCode(HttpServletResponse response, int code) {
    response.setStatus(code);
    response.setContentType("application/json");
    response.setCharacterEncoding("utf-8");
    response.setHeader("Access-Control-Allow-Origin", "*");
    response.setHeader("Access-Control-Request-Method", "OPTIONS, GET, POST");
  }

  private void returnProgress(HttpServletResponse response, OperationFuture future) throws IOException {
    String progressString = future.progressString();
    setResponseCode(response, SC_OK);
    response.setContentLength(progressString.length());
    response.getOutputStream().write(progressString.getBytes(StandardCharsets.UTF_8));
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
      return new GoalsAndRequirements(ignoreCache ? allGoals : Collections.emptyList(),
                                      getRequirements(VALID_WINDOWS));
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
