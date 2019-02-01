/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import com.linkedin.kafka.cruisecontrol.servlet.EndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;
import com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager;
import com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlState;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerEvenRackAwareGoal;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
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
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.currentUtcDate;
import static com.linkedin.kafka.cruisecontrol.servlet.EndPoint.*;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.REQUEST_URI;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.getClientIpAddress;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.writeErrorResponse;
import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST;


/**
 * The util class for Kafka Cruise Control parameters.
 */
public class ParameterUtils {
  public static final String JSON_PARAM = "json";
  public static final String START_MS_PARAM = "start";
  public static final String END_MS_PARAM = "end";
  public static final String ENTRIES_PARAM = "entries";
  public static final String ALLOW_CAPACITY_ESTIMATION_PARAM = "allow_capacity_estimation";
  public static final String CLEAR_METRICS_PARAM = "clearmetrics";
  public static final String TIME_PARAM = "time";
  public static final String VERBOSE_PARAM = "verbose";
  public static final String SUPER_VERBOSE_PARAM = "super_verbose";
  public static final String RESOURCE_PARAM = "resource";
  public static final String REASON = "reason";
  public static final String DATA_FROM_PARAM = "data_from";
  public static final String KAFKA_ASSIGNER_MODE_PARAM = "kafka_assigner";
  public static final String MAX_LOAD_PARAM = "max_load";
  public static final String GOALS_PARAM = "goals";
  public static final String BROKER_ID_PARAM = "brokerid";
  public static final String TOPIC_PARAM = "topic";
  public static final String PARTITION_PARAM = "partition";
  public static final String DRY_RUN_PARAM = "dryrun";
  public static final String THROTTLE_ADDED_BROKER_PARAM = "throttle_added_broker";
  public static final String THROTTLE_REMOVED_BROKER_PARAM = "throttle_removed_broker";
  public static final String IGNORE_PROPOSAL_CACHE_PARAM = "ignore_proposal_cache";
  public static final String USE_READY_DEFAULT_GOALS_PARAM = "use_ready_default_goals";
  public static final String CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_PARAM = "concurrent_partition_movements_per_broker";
  public static final String CONCURRENT_LEADER_MOVEMENTS_PARAM = "concurrent_leader_movements";
  public static final String DEFAULT_PARTITION_LOAD_RESOURCE = "disk";
  public static final String SUBSTATES_PARAM = "substates";
  public static final String MIN_VALID_PARTITION_RATIO_PARAM = "min_valid_partition_ratio";
  public static final String SKIP_HARD_GOAL_CHECK_PARAM = "skip_hard_goal_check";
  public static final String EXCLUDED_TOPICS_PARAM = "excluded_topics";
  public static final String USER_TASK_IDS_PARAM = "user_task_ids";
  public static final String CLIENT_IDS_PARAM = "client_ids";
  public static final String ENDPOINTS_PARAM = "endpoints";
  public static final String TYPES_PARAM = "types";
  public static final String SKIP_URP_DEMOTION_PARAM = "skip_urp_demotion";
  public static final String EXCLUDE_FOLLOWER_DEMOTION_PARAM = "exclude_follower_demotion";
  public static final String DISABLE_SELF_HEALING_FOR_PARAM = "disable_self_healing_for";
  public static final String ENABLE_SELF_HEALING_FOR_PARAM = "enable_self_healing_for";
  public static final String EXCLUDE_RECENTLY_DEMOTED_BROKERS_PARAM = "exclude_recently_demoted_brokers";
  public static final String EXCLUDE_RECENTLY_REMOVED_BROKERS_PARAM = "exclude_recently_removed_brokers";

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
    proposals.add(EXCLUDE_RECENTLY_DEMOTED_BROKERS_PARAM);
    proposals.add(EXCLUDE_RECENTLY_REMOVED_BROKERS_PARAM);

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
    addOrRemoveBroker.add(VERBOSE_PARAM);
    addOrRemoveBroker.add(EXCLUDE_RECENTLY_DEMOTED_BROKERS_PARAM);
    addOrRemoveBroker.add(EXCLUDE_RECENTLY_REMOVED_BROKERS_PARAM);

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
    demoteBroker.add(VERBOSE_PARAM);
    demoteBroker.add(SKIP_URP_DEMOTION_PARAM);
    demoteBroker.add(EXCLUDE_FOLLOWER_DEMOTION_PARAM);
    demoteBroker.add(EXCLUDE_RECENTLY_DEMOTED_BROKERS_PARAM);

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
    rebalance.add(VERBOSE_PARAM);
    rebalance.add(EXCLUDE_RECENTLY_DEMOTED_BROKERS_PARAM);
    rebalance.add(EXCLUDE_RECENTLY_REMOVED_BROKERS_PARAM);

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
    userTasks.add(CLIENT_IDS_PARAM);
    userTasks.add(ENTRIES_PARAM);
    userTasks.add(ENDPOINTS_PARAM);
    userTasks.add(TYPES_PARAM);

    Set<String> admin = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    admin.add(JSON_PARAM);
    admin.add(DISABLE_SELF_HEALING_FOR_PARAM);
    admin.add(ENABLE_SELF_HEALING_FOR_PARAM);
    admin.add(CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_PARAM);
    admin.add(CONCURRENT_LEADER_MOVEMENTS_PARAM);

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

  private ParameterUtils() {
  }

  public static EndPoint endPoint(HttpServletRequest request) {
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

  static void handleParameterParseException(Exception e, HttpServletResponse response, String errorMsg, boolean json)
      throws IOException {
    StringWriter sw = new StringWriter();
    e.printStackTrace(new PrintWriter(sw));
    writeErrorResponse(response, sw.toString(), errorMsg, SC_BAD_REQUEST, json);
  }

  public static boolean hasValidParameters(HttpServletRequest request, HttpServletResponse response) throws IOException {
    EndPoint endPoint = endPoint(request);
    Set<String> validParamNames = VALID_ENDPOINT_PARAM_NAMES.get(endPoint);
    Set<String> userParams = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    userParams.addAll(request.getParameterMap().keySet());
    if (validParamNames != null) {
      userParams.removeAll(validParamNames);
    }

    if (!userParams.isEmpty()) {
      // User request specifies parameters that are not a subset of the valid parameters.
      String errorResp = String.format("Unrecognized endpoint parameters in %s %s request: %s.",
                                       endPoint, request.getMethod(), userParams.toString());
      writeErrorResponse(response, "", errorResp, SC_BAD_REQUEST, wantJSON(request));
      return false;
    }
    return true;
  }

  private static boolean getBooleanParam(HttpServletRequest request, String parameter, boolean defaultIfMissing) {
    String parameterString = request.getParameter(parameter);
    return parameterString == null ? defaultIfMissing : Boolean.parseBoolean(parameterString);
  }

  public static boolean wantJSON(HttpServletRequest request) {
    return getBooleanParam(request, JSON_PARAM, false);
  }

  static boolean allowCapacityEstimation(HttpServletRequest request) {
    return getBooleanParam(request, ALLOW_CAPACITY_ESTIMATION_PARAM, true);
  }

  private static boolean excludeBrokers(HttpServletRequest request, String parameter, boolean defaultIfMissing) {
    boolean isKafkaAssignerMode = getMode(request);
    boolean excludeBrokers = getBooleanParam(request, parameter, defaultIfMissing);
    if (isKafkaAssignerMode && excludeBrokers) {
      throw new UserRequestException("Kafka assigner mode does not support excluding brokers.");
    }

    return excludeBrokers;
  }

  /**
   * Default: false -- i.e. a recently demoted broker may receive leadership from the other brokers.
   */
  static boolean excludeRecentlyDemotedBrokers(HttpServletRequest request) {
    return excludeBrokers(request, EXCLUDE_RECENTLY_DEMOTED_BROKERS_PARAM, false);
  }

  /**
   * Default: false -- i.e. a recently removed broker may receive replicas from the other brokers.
   */
  static boolean excludeRecentlyRemovedBrokers(HttpServletRequest request) {
    return excludeBrokers(request, EXCLUDE_RECENTLY_REMOVED_BROKERS_PARAM, false);
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
    return resourceString == null ? DEFAULT_PARTITION_LOAD_RESOURCE : resourceString;
  }

  static String reason(HttpServletRequest request) {
    String reason = request.getParameter(REASON);
    String ip = getClientIpAddress(request);
    return String.format("%s (Client: %s, Date: %s)", reason == null ? "No reason provided" : reason, ip, currentUtcDate());
  }

  private static Set<String> parseParamToStringSet(HttpServletRequest request, String param) throws UnsupportedEncodingException {
    String paramString = urlDecode(request.getParameter(param));
    Set<String> paramsString = paramString == null ? new HashSet<>(0)
                                                   : new HashSet<>(Arrays.asList(paramString.split(",")));
    paramsString.removeIf(String::isEmpty);
    return paramsString;
  }

  /**
   * Empty parameter means all substates are requested.
   */
  static Set<CruiseControlState.SubState> substates(HttpServletRequest request) throws UnsupportedEncodingException {
    Set<String> substatesString = parseParamToStringSet(request, SUBSTATES_PARAM);

    Set<CruiseControlState.SubState> substates = new HashSet<>(substatesString.size());
    try {
      for (String substateString : substatesString) {
        substates.add(CruiseControlState.SubState.valueOf(substateString.toUpperCase()));
      }
    } catch (IllegalArgumentException iae) {
      throw new IllegalArgumentException(String.format("Unsupported substates in %s. Supported: %s",
                                                       substatesString, CruiseControlState.SubState.cachedValues()));
    }

    return Collections.unmodifiableSet(substates);
  }

  private static Set<AnomalyType> anomalyTypes(HttpServletRequest request, boolean isEnable) throws UnsupportedEncodingException {
    String selfHealingForParam = urlDecode(request.getParameter(isEnable ? ENABLE_SELF_HEALING_FOR_PARAM : DISABLE_SELF_HEALING_FOR_PARAM));
    Set<String> selfHealingForString = selfHealingForParam == null ? new HashSet<>(0)
                                                                   : new HashSet<>(Arrays.asList(selfHealingForParam.split(",")));
    selfHealingForString.removeIf(String::isEmpty);

    Set<AnomalyType> anomalyTypes = new HashSet<>(selfHealingForString.size());
    try {
      for (String shfString : selfHealingForString) {
        anomalyTypes.add(AnomalyType.valueOf(shfString.toUpperCase()));
      }
    } catch (IllegalArgumentException iae) {
      throw new IllegalArgumentException(String.format("Unsupported anomaly types in %s. Supported: %s",
                                                       selfHealingForString, AnomalyType.cachedValues()));
    }

    return Collections.unmodifiableSet(anomalyTypes);
  }

  /**
   * Get self healing types for {@link #ENABLE_SELF_HEALING_FOR_PARAM} and {@link #DISABLE_SELF_HEALING_FOR_PARAM}.
   *
   * Sanity check ensures that the same anomaly is not specified in both configs at the same request.
   */
  static Map<Boolean, Set<AnomalyType>> selfHealingFor(HttpServletRequest request) throws UnsupportedEncodingException {
    Set<AnomalyType> enableSelfHealingFor = anomalyTypes(request, true);
    Set<AnomalyType> disableSelfHealingFor = anomalyTypes(request, false);

    // Sanity check: Ensure that the same anomaly is not specified in both configs at the same request.
    Set<AnomalyType> intersection = new HashSet<>(enableSelfHealingFor);
    intersection.retainAll(disableSelfHealingFor);
    if (!intersection.isEmpty()) {
      throw new IllegalArgumentException(String.format("The same anomaly cannot be specified in both disable and"
                                                       + "enable parameters. Intersection: %s", intersection));
    }

    Map<Boolean, Set<AnomalyType>> selfHealingFor = new HashMap<>(2);
    selfHealingFor.put(true, enableSelfHealingFor);
    selfHealingFor.put(false, disableSelfHealingFor);

    return selfHealingFor;
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

  public static int entries(HttpServletRequest request) {
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
  public static Set<UUID> userTaskIds(HttpServletRequest request) throws UnsupportedEncodingException {
    String userTaskIdsString = urlDecode(request.getParameter(USER_TASK_IDS_PARAM));
    return userTaskIdsString == null ? Collections.emptySet()
                                     : Arrays.stream(userTaskIdsString.split(",")).map(UUID::fromString)
                                             .collect(Collectors.toSet());
  }

  /**
   * Default: An empty set.
   */
  public static Set<String> clientIds(HttpServletRequest request) throws UnsupportedEncodingException {
    Set<String> parsedClientIds = parseParamToStringSet(request, CLIENT_IDS_PARAM);
    // May need to validate clientIds
    return Collections.unmodifiableSet(parsedClientIds);
  }

  /**
   * Default: An empty set.
   */
  public static Set<EndPoint> endPoints(HttpServletRequest request) throws UnsupportedEncodingException {
    Set<String> parsedEndPoints = parseParamToStringSet(request, ENDPOINTS_PARAM);

    Set<EndPoint> endPoints = new HashSet<>();
    for (EndPoint endPoint : EndPoint.cachedValues()) {
      if (parsedEndPoints.contains(endPoint.toString())) {
        endPoints.add(endPoint);
      }
    }
    return Collections.unmodifiableSet(endPoints);
  }

  /**
   * Default: An empty set.
   */
  public static Set<UserTaskManager.TaskState> types(HttpServletRequest request) throws UnsupportedEncodingException {
    Set<String> parsedTaskStates = parseParamToStringSet(request, TYPES_PARAM);

    Set<UserTaskManager.TaskState> taskStates = new HashSet<>();
    for (UserTaskManager.TaskState state : UserTaskManager.TaskState.cachedValues()) {
      if (parsedTaskStates.contains(state.toString())) {
        taskStates.add(state);
      }
    }
    return Collections.unmodifiableSet(taskStates);
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

  static boolean skipUrpDemotion(HttpServletRequest request) {
    return getBooleanParam(request, SKIP_URP_DEMOTION_PARAM, false);
  }

  static boolean excludeFollowerDemotion(HttpServletRequest request) {
    return getBooleanParam(request, EXCLUDE_FOLLOWER_DEMOTION_PARAM, false);
  }

  public enum DataFrom {
    VALID_WINDOWS, VALID_PARTITIONS
  }
}
