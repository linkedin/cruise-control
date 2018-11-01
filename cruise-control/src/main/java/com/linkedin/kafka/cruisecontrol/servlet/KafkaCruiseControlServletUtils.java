/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlState;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerEvenRackAwareGoal;
import com.linkedin.kafka.cruisecontrol.async.OperationFuture;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static com.linkedin.kafka.cruisecontrol.servlet.EndPoint.*;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST;

/**
 * The util class for Kafka Cruise Control servlet.
 */
class KafkaCruiseControlServletUtils {
  static final int JSON_VERSION = 1;
  private static final String VERSION = "version";
  private static final String MESSAGE = "message";
  private static final String STACK_TRACE = "stackTrace";
  private static final String ERROR_MESSAGE = "errorMessage";
  private static final String PROGRESS = "progress";
  private static final String REQUEST_URI = "/KAFKACRUISECONTROL/";

  private static final String JSON_PARAM = "json";
  private static final String START_MS_PARAM = "start";
  private static final String END_MS_PARAM = "end";
  private static final String ENTRIES_PARAM = "entries";
  private static final String ALLOW_CAPACITY_ESTIMATION_PARAM = "allow_capacity_estimation";
  private static final String CLEAR_METRICS_PARAM = "clearmetrics";
  private static final String TIME_PARAM = "time";
  private static final String VERBOSE_PARAM = "verbose";
  private static final String SUPER_VERBOSE_PARAM = "super_verbose";
  private static final String RESOURCE_PARAM = "resource";
  private static final String DATA_FROM_PARAM = "data_from";
  private static final String KAFKA_ASSIGNER_MODE_PARAM = "kafka_assigner";
  private static final String MAX_LOAD_PARAM = "max_load";
  private static final String GOALS_PARAM = "goals";
  private static final String BROKER_ID_PARAM = "brokerid";
  private static final String TOPIC_PARAM = "topic";
  private static final String PARTITION_PARAM = "partition";
  private static final String DRY_RUN_PARAM = "dryrun";
  private static final String THROTTLE_ADDED_BROKER_PARAM = "throttle_added_broker";
  private static final String THROTTLE_REMOVED_BROKER_PARAM = "throttle_removed_broker";
  private static final String IGNORE_PROPOSAL_CACHE_PARAM = "ignore_proposal_cache";
  private static final String USE_READY_DEFAULT_GOALS_PARAM = "use_ready_default_goals";
  private static final String CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_PARAM = "concurrent_partition_movements_per_broker";
  private static final String CONCURRENT_LEADER_MOVEMENTS_PARAM = "concurrent_leader_movements";
  private static final String DEFAULT_PARTITION_LOAD_RESOURCE = "disk";
  private static final String SUBSTATES_PARAM = "substates";
  private static final String MIN_VALID_PARTITION_RATIO_PARAM = "min_valid_partition_ratio";
  private static final String SKIP_HARD_GOAL_CHECK_PARAM = "skip_hard_goal_check";
  private static final String EXCLUDED_TOPICS_PARAM = "excluded_topics";
  private static final String USER_TASK_IDS_PARAM = "user_task_ids";

  static final Map<EndPoint, Set<String>> VALID_ENDPOINT_PARAM_NAMES;
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
    load.add(JSON_PARAM);
    load.add(ALLOW_CAPACITY_ESTIMATION_PARAM);

    Set<String> partitionLoad = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    partitionLoad.add(RESOURCE_PARAM);
    partitionLoad.add(START_MS_PARAM);
    partitionLoad.add(END_MS_PARAM);
    partitionLoad.add(ENTRIES_PARAM);
    partitionLoad.add(JSON_PARAM);
    partitionLoad.add(ALLOW_CAPACITY_ESTIMATION_PARAM);
    partitionLoad.add(MAX_LOAD_PARAM);
    partitionLoad.add(TOPIC_PARAM);
    partitionLoad.add(PARTITION_PARAM);
    partitionLoad.add(MIN_VALID_PARTITION_RATIO_PARAM);

    Set<String> proposals = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    proposals.add(VERBOSE_PARAM);
    proposals.add(IGNORE_PROPOSAL_CACHE_PARAM);
    proposals.add(DATA_FROM_PARAM);
    proposals.add(GOALS_PARAM);
    proposals.add(KAFKA_ASSIGNER_MODE_PARAM);
    proposals.add(JSON_PARAM);
    proposals.add(ALLOW_CAPACITY_ESTIMATION_PARAM);
    proposals.add(EXCLUDED_TOPICS_PARAM);
    proposals.add(USE_READY_DEFAULT_GOALS_PARAM);

    Set<String> state = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    state.add(VERBOSE_PARAM);
    state.add(SUPER_VERBOSE_PARAM);
    state.add(JSON_PARAM);
    state.add(SUBSTATES_PARAM);

    Set<String> addOrRemoveBroker = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    addOrRemoveBroker.add(BROKER_ID_PARAM);
    addOrRemoveBroker.add(DRY_RUN_PARAM);
    addOrRemoveBroker.add(DATA_FROM_PARAM);
    addOrRemoveBroker.add(GOALS_PARAM);
    addOrRemoveBroker.add(KAFKA_ASSIGNER_MODE_PARAM);
    addOrRemoveBroker.add(JSON_PARAM);
    addOrRemoveBroker.add(ALLOW_CAPACITY_ESTIMATION_PARAM);
    addOrRemoveBroker.add(CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_PARAM);
    addOrRemoveBroker.add(CONCURRENT_LEADER_MOVEMENTS_PARAM);
    addOrRemoveBroker.add(SKIP_HARD_GOAL_CHECK_PARAM);
    addOrRemoveBroker.add(EXCLUDED_TOPICS_PARAM);
    addOrRemoveBroker.add(USE_READY_DEFAULT_GOALS_PARAM);

    Set<String> addBroker = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    addBroker.add(THROTTLE_ADDED_BROKER_PARAM);
    addBroker.addAll(addOrRemoveBroker);

    Set<String> removeBroker = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    removeBroker.add(THROTTLE_REMOVED_BROKER_PARAM);
    removeBroker.addAll(addOrRemoveBroker);

    Set<String> demoteBroker = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    demoteBroker.add(BROKER_ID_PARAM);
    demoteBroker.add(DRY_RUN_PARAM);
    demoteBroker.add(JSON_PARAM);
    demoteBroker.add(ALLOW_CAPACITY_ESTIMATION_PARAM);
    demoteBroker.add(CONCURRENT_LEADER_MOVEMENTS_PARAM);

    Set<String> rebalance = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    rebalance.add(DRY_RUN_PARAM);
    rebalance.add(GOALS_PARAM);
    rebalance.add(KAFKA_ASSIGNER_MODE_PARAM);
    rebalance.add(DATA_FROM_PARAM);
    rebalance.add(JSON_PARAM);
    rebalance.add(ALLOW_CAPACITY_ESTIMATION_PARAM);
    rebalance.add(CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_PARAM);
    rebalance.add(CONCURRENT_LEADER_MOVEMENTS_PARAM);
    rebalance.add(SKIP_HARD_GOAL_CHECK_PARAM);
    rebalance.add(EXCLUDED_TOPICS_PARAM);
    rebalance.add(USE_READY_DEFAULT_GOALS_PARAM);

    Set<String> kafkaClusterState = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    kafkaClusterState.add(VERBOSE_PARAM);
    kafkaClusterState.add(JSON_PARAM);

    Set<String> pauseSampling = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    pauseSampling.add(JSON_PARAM);

    Set<String> resumeSampling = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    resumeSampling.add(JSON_PARAM);

    Set<String> stopProposalExecution = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    stopProposalExecution.add(JSON_PARAM);

    Set<String> userTasks = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    userTasks.add(JSON_PARAM);
    userTasks.add(USER_TASK_IDS_PARAM);

    Set<String> admin = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    admin.add(JSON_PARAM);

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
    validParamNames.put(STOP_PROPOSAL_EXECUTION, Collections.unmodifiableSet(stopProposalExecution));
    validParamNames.put(PAUSE_SAMPLING, Collections.unmodifiableSet(pauseSampling));
    validParamNames.put(RESUME_SAMPLING, Collections.unmodifiableSet(resumeSampling));
    validParamNames.put(KAFKA_CLUSTER_STATE, Collections.unmodifiableSet(kafkaClusterState));
    validParamNames.put(USER_TASKS, Collections.unmodifiableSet(userTasks));
    validParamNames.put(ADMIN, Collections.unmodifiableSet(admin));

    VALID_ENDPOINT_PARAM_NAMES = Collections.unmodifiableMap(validParamNames);
  }

  private KafkaCruiseControlServletUtils() {

  }

  public static final String[] HEADERS_TO_TRY = {
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

  static EndPoint endPoint(HttpServletRequest request) {
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

    String path = request.getRequestURI().toUpperCase().replace(REQUEST_URI, "");
    for (EndPoint endPoint : supportedEndpoints) {
      if (endPoint.toString().equalsIgnoreCase(path)) {
        return endPoint;
      }
    }
    return null;
  }

  static String getStateUrl(HttpServletRequest request) {
    String url = request.getRequestURL().toString().toUpperCase();
    int pos = url.indexOf(REQUEST_URI);
    return url.substring(0, pos + REQUEST_URI.length()) + STATE;
  }

  private static boolean getBooleanParam(HttpServletRequest request, String parameter, boolean defaultIfMissing) {
    String parameterString = request.getParameter(parameter);
    return parameterString == null ? defaultIfMissing : Boolean.parseBoolean(parameterString);
  }

  static boolean wantJSON(HttpServletRequest request) {
    return getBooleanParam(request, JSON_PARAM, false);
  }

  static boolean allowCapacityEstimation(HttpServletRequest request) {
    return getBooleanParam(request, ALLOW_CAPACITY_ESTIMATION_PARAM, true);
  }

  static boolean wantMaxLoad(HttpServletRequest request) {
    return getBooleanParam(request, MAX_LOAD_PARAM, false);
  }

  static boolean isVerbose(HttpServletRequest request) {
    return getBooleanParam(request, VERBOSE_PARAM, false);
  }

  static boolean isSuperVerbose(HttpServletRequest request) {
    return getBooleanParam(request, SUPER_VERBOSE_PARAM, false);
  }

  static boolean clearMetrics(HttpServletRequest request) {
    return getBooleanParam(request, CLEAR_METRICS_PARAM, true);
  }

  private static boolean getMode(HttpServletRequest request) {
    return getBooleanParam(request, KAFKA_ASSIGNER_MODE_PARAM, false);
  }

  static boolean ignoreProposalCache(HttpServletRequest request) {
    return getBooleanParam(request, IGNORE_PROPOSAL_CACHE_PARAM, false);
  }

  static boolean useReadyDefaultGoals(HttpServletRequest request) {
    boolean useReadyDefaultGoals = getBooleanParam(request, USE_READY_DEFAULT_GOALS_PARAM, false);
    // Ensure that goals parameter is not specified.
    if (useReadyDefaultGoals && request.getParameter(GOALS_PARAM) != null) {
      throw new UserRequestException("Cannot specify " + USE_READY_DEFAULT_GOALS_PARAM + " parameter when explicitly "
                                     + "specifying goals in request.");
    }
    return useReadyDefaultGoals;
  }

  static boolean getDryRun(HttpServletRequest request) {
    return getBooleanParam(request, DRY_RUN_PARAM, true);
  }

  static boolean throttleAddedOrRemovedBrokers(HttpServletRequest request, EndPoint endPoint) {
    return endPoint == ADD_BROKER ? getBooleanParam(request, THROTTLE_ADDED_BROKER_PARAM, true)
                                  : getBooleanParam(request, THROTTLE_REMOVED_BROKER_PARAM, true);
  }

  static long time(HttpServletRequest request) {
    String timeString = request.getParameter(TIME_PARAM);
    return (timeString == null || timeString.toUpperCase().equals("NOW")) ? System.currentTimeMillis()
                                                                          : Long.parseLong(timeString);
  }

  static Long startMs(HttpServletRequest request) {
    String startMsString = request.getParameter(START_MS_PARAM);
    return startMsString == null ? null : Long.valueOf(startMsString);
  }

  static Long endMs(HttpServletRequest request) {
    String endMsString = request.getParameter(END_MS_PARAM);
    return endMsString == null ? null : Long.valueOf(endMsString);
  }

  static Pattern topic(HttpServletRequest request) {
    String topicString = request.getParameter(TOPIC_PARAM);
    return topicString != null ? Pattern.compile(topicString) : null;
  }

  static Double minValidPartitionRatio(HttpServletRequest request) {
    String minValidPartitionRatioString = request.getParameter(MIN_VALID_PARTITION_RATIO_PARAM);
    if (minValidPartitionRatioString == null) {
      return null;
    } else {
      Double minValidPartitionRatio = Double.parseDouble(minValidPartitionRatioString);
      if (minValidPartitionRatio > 1.0 || minValidPartitionRatio < 0.0) {
        throw new IllegalArgumentException("The requested minimum partition ratio must be in range [0.0, 1.0] (Requested: "
                                           + minValidPartitionRatio.toString() + ").");
      }
      return minValidPartitionRatio;
    }
  }

  static String resourceString(HttpServletRequest request) {
    String resourceString = request.getParameter(RESOURCE_PARAM);
    if (resourceString == null) {
      resourceString = DEFAULT_PARTITION_LOAD_RESOURCE;
    }
    return resourceString;
  }

  /**
   * Empty parameter means all substates are requested.
   */
  static Set<KafkaCruiseControlState.SubState> substates(HttpServletRequest request) throws UnsupportedEncodingException {
    Set<String> substatesString;
    String substatesParam = urlDecode(request.getParameter(SUBSTATES_PARAM));
    substatesString = substatesParam == null ? new HashSet<>() : new HashSet<>(Arrays.asList(substatesParam.split(",")));
    substatesString.removeIf(String::isEmpty);

    Set<KafkaCruiseControlState.SubState> substates = new HashSet<>();
    try {
      for (String substateString : substatesString) {
        substates.add(KafkaCruiseControlState.SubState.valueOf(substateString.toUpperCase()));
      }
    } catch (IllegalArgumentException iae) {
      throw new IllegalArgumentException(
          String.format("Unsupported substates in %s. Supported: %s", substatesString, Arrays.toString(
              KafkaCruiseControlState.SubState.values())));
    }

    return Collections.unmodifiableSet(substates);
  }

  static String urlEncode(String s) throws UnsupportedEncodingException {
    return s == null ? null : URLEncoder.encode(s, StandardCharsets.UTF_8.name());
  }

  static String urlDecode(String s) throws UnsupportedEncodingException {
    return s == null ? null : URLDecoder.decode(s, StandardCharsets.UTF_8.name());
  }

  static List<String> getGoals(HttpServletRequest request) throws UnsupportedEncodingException {
    boolean isKafkaAssignerMode = getMode(request);
    List<String> goals;
    String goalsString = urlDecode(request.getParameter(GOALS_PARAM));
    goals = goalsString == null ? new ArrayList<>() : Arrays.asList(goalsString.split(","));
    goals.removeIf(String::isEmpty);

    // KafkaAssigner mode is assumed to use two KafkaAssigner goals, if client specifies goals in request, throw exception.
    if (isKafkaAssignerMode) {
      if (!goals.isEmpty()) {
        throw new UserRequestException("Kafka assigner mode does not support explicitly specifying goals in request.");
      }
      goals = Arrays.asList(KafkaAssignerEvenRackAwareGoal.class.getSimpleName(),
                            KafkaAssignerDiskUsageDistributionGoal.class.getSimpleName());
    }
    return Collections.unmodifiableList(goals);
  }

  static int entries(HttpServletRequest request) {
    String entriesString = request.getParameter(ENTRIES_PARAM);
    return entriesString == null ? Integer.MAX_VALUE : Integer.parseInt(entriesString);
  }

  /**
   * @param isPartitionMovement True if partition movement per broker, false if the total leader movement.
   */
  static Integer concurrentMovements(HttpServletRequest request, boolean isPartitionMovement) {
    String concurrentMovementsPerBrokerString = isPartitionMovement
                                                ? request.getParameter(CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_PARAM)
                                                : request.getParameter(CONCURRENT_LEADER_MOVEMENTS_PARAM);
    if (concurrentMovementsPerBrokerString == null) {
      return null;
    }
    Integer concurrentMovementsPerBroker = Integer.parseInt(concurrentMovementsPerBrokerString);
    if (concurrentMovementsPerBroker <= 0) {
      throw new IllegalArgumentException("The requested movement concurrency must be positive (Requested: "
                                         + concurrentMovementsPerBroker.toString() + ").");
    }

    return concurrentMovementsPerBroker;
  }

  static Pattern excludedTopics(HttpServletRequest request) {
    String excludedTopicsString = request.getParameter(EXCLUDED_TOPICS_PARAM);
    return excludedTopicsString != null ? Pattern.compile(excludedTopicsString) : null;
  }

  /**
   * @param isUpperBound True if upper bound, false if lower bound.
   */
  static int partitionBoundary(HttpServletRequest request, boolean isUpperBound) {
    String partitionString = request.getParameter(PARTITION_PARAM);
    if (partitionString == null) {
      return isUpperBound ? Integer.MAX_VALUE : Integer.MIN_VALUE;
    } else if (!partitionString.contains("-")) {
      return Integer.parseInt(partitionString);
    }

    String[] boundaries = partitionString.split("-");
    if (boundaries.length > 2) {
      throw new IllegalArgumentException("The " + PARTITION_PARAM + " parameter cannot contain multiple dashes.");
    }
    return Integer.parseInt(boundaries[isUpperBound ? 1 : 0]);
  }

  static List<Integer> brokerIds(HttpServletRequest request) throws UnsupportedEncodingException {
    List<Integer> brokerIds = new ArrayList<>();
    String brokersString = urlDecode(request.getParameter(BROKER_ID_PARAM));
    if (brokersString != null) {
      brokerIds = Arrays.stream(brokersString.split(",")).map(Integer::parseInt).collect(Collectors.toList());
    }
    if (brokerIds.isEmpty()) {
      throw new IllegalArgumentException("Target broker ID is not provided.");
    }
    return Collections.unmodifiableList(brokerIds);
  }

  /**
   * Default: An empty set.
   */
  static Set<UUID> userTaskIds(HttpServletRequest request) throws UnsupportedEncodingException {
    String userTaskIdsString = urlDecode(request.getParameter(USER_TASK_IDS_PARAM));
    return userTaskIdsString == null ? Collections.emptySet()
                                     : Arrays.stream(userTaskIdsString.split(",")).map(UUID::fromString)
                                             .collect(Collectors.toSet());
  }

  /**
   * Default: {@link DataFrom#VALID_WINDOWS}
   */
  static DataFrom getDataFrom(HttpServletRequest request) {
    DataFrom dataFrom = DataFrom.VALID_WINDOWS;
    String dataFromString = request.getParameter(DATA_FROM_PARAM);
    if (dataFromString != null) {
      dataFrom = DataFrom.valueOf(dataFromString.toUpperCase());
    }
    return dataFrom;
  }

  /**
   * Skip hard goal check in kafka_assigner mode,
   */
  static boolean skipHardGoalCheck(HttpServletRequest request) {
    return getMode(request) || getBooleanParam(request, SKIP_HARD_GOAL_CHECK_PARAM, false);
  }

  static void setResponseCode(HttpServletResponse response, int code, boolean json) {
    response.setStatus(code);
    response.setContentType(json ? "application/json" : "text/plain");
    response.setCharacterEncoding(StandardCharsets.UTF_8.name());
    response.setHeader("Access-Control-Allow-Origin", "*");
    response.setHeader("Access-Control-Request-Method", "OPTIONS, GET, POST");
  }

  static void writeSuccessResponse(HttpServletResponse response,
                                   Supplier<String> jsonStringSupplier,
                                   Consumer<OutputStream> plaintextWriter,
                                   boolean json) throws IOException {
    OutputStream out = response.getOutputStream();
    setResponseCode(response, SC_OK, json);
    if (json) {
      String jsonString = jsonStringSupplier.get();
      response.setContentLength(jsonString.length());
      out.write(jsonString.getBytes(StandardCharsets.UTF_8));
    } else {
      plaintextWriter.accept(out);
    }
    out.flush();
  }

  static ModelCompletenessRequirements getRequirements(DataFrom dataFrom) {
    return dataFrom == DataFrom.VALID_PARTITIONS ? new ModelCompletenessRequirements(Integer.MAX_VALUE, 0.0, true)
                                                 : new ModelCompletenessRequirements(1, 1.0, true);
  }

  static void writeResponseToOutputStream(HttpServletResponse response,
                                          int responseCode,
                                          boolean json,
                                          String responseMsg)
      throws IOException {
    OutputStream out = response.getOutputStream();
    setResponseCode(response, responseCode, json);
    response.setContentLength(responseMsg.length());
    out.write(responseMsg.getBytes(StandardCharsets.UTF_8));
    out.flush();
  }

  static void setErrorResponse(HttpServletResponse response,
                               String stackTrace,
                               String errorMessage,
                               int responseCode,
                               boolean json)
      throws IOException {
    String responseMsg;
    if (json) {
      Map<String, Object> exceptionMap = new HashMap<>();
      exceptionMap.put(VERSION, JSON_VERSION);
      exceptionMap.put(STACK_TRACE, stackTrace);
      exceptionMap.put(ERROR_MESSAGE, errorMessage);
      Gson gson = new Gson();
      responseMsg = gson.toJson(exceptionMap);
    } else {
      responseMsg = errorMessage == null ? "" : errorMessage;
    }
    writeResponseToOutputStream(response, responseCode, json, responseMsg);
  }

  static void setSuccessResponse(HttpServletResponse response, String message, boolean json) throws IOException {
    String responseMsg;
    if (json) {
      Map<String, Object> respMap = new HashMap<>();
      respMap.put(VERSION, JSON_VERSION);
      respMap.put(MESSAGE, message);
      Gson gson = new Gson();
      responseMsg = gson.toJson(respMap);
    } else {
      responseMsg = message == null ? "" : message;
    }
    writeResponseToOutputStream(response, SC_OK, json, responseMsg);
  }

  static void handleParameterParseException(Exception e, HttpServletResponse response, String errorMsg, boolean json)
      throws IOException {
    StringWriter sw = new StringWriter();
    e.printStackTrace(new PrintWriter(sw));
    setErrorResponse(response, sw.toString(), errorMsg, SC_BAD_REQUEST, json);
  }

  static void returnProgress(HttpServletResponse response, OperationFuture future, boolean json) throws IOException {
    String responseMsg;
    if (json) {
      Map<String, Object> respMap = new HashMap<>();
      respMap.put(VERSION, JSON_VERSION);
      respMap.put(PROGRESS, future.getJsonArray());
      Gson gson = new GsonBuilder().serializeNulls().serializeSpecialFloatingPointValues().create();
       responseMsg = gson.toJson(respMap);
    } else {
       responseMsg = future.progressString();
    }
    writeResponseToOutputStream(response, SC_OK, json, responseMsg);
  }

  enum DataFrom {
    VALID_WINDOWS, VALID_PARTITIONS
  }
}
