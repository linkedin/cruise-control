/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.cruisecontrol.servlet.EndPoint;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import com.linkedin.kafka.cruisecontrol.executor.strategy.BaseReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;
import com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager;
import com.linkedin.kafka.cruisecontrol.servlet.purgatory.ReviewStatus;
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
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.currentUtcDate;
import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.*;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.GET_METHOD;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.POST_METHOD;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.REQUEST_URI;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.getClientIpAddress;
import static com.linkedin.kafka.cruisecontrol.servlet.purgatory.ReviewStatus.APPROVED;
import static com.linkedin.kafka.cruisecontrol.servlet.purgatory.ReviewStatus.DISCARDED;
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
  public static final String REASON_PARAM = "reason";
  public static final String DATA_FROM_PARAM = "data_from";
  public static final String KAFKA_ASSIGNER_MODE_PARAM = "kafka_assigner";
  public static final String MAX_LOAD_PARAM = "max_load";
  public static final String AVG_LOAD_PARAM = "avg_load";
  public static final String GOALS_PARAM = "goals";
  public static final String BROKER_ID_PARAM = "brokerid";
  public static final String DROP_RECENTLY_REMOVED_BROKERS_PARAM = "drop_recently_removed_brokers";
  public static final String DROP_RECENTLY_DEMOTED_BROKERS_PARAM = "drop_recently_demoted_brokers";
  public static final String DESTINATION_BROKER_IDS_PARAM = "destination_broker_ids";
  public static final String REVIEW_ID_PARAM = "review_id";
  public static final String REVIEW_IDS_PARAM = "review_ids";
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
  public static final String REPLICA_MOVEMENT_STRATEGIES_PARAM = "replica_movement_strategies";
  public static final String APPROVE_PARAM = "approve";
  public static final String DISCARD_PARAM = "discard";
  public static final String REPLICATION_FACTOR_PARAM = "replication_factor";
  public static final String SKIP_RACK_AWARENESS_CHECK_PARAM = "skip_rack_awareness_check";
  public static final String FETCH_COMPLETED_TASK_PARAM = "fetch_completed_task";
  private static final int MAX_REASON_LENGTH = 50;
  public static final long DEFAULT_START_TIME_FOR_CLUSTER_MODEL = -1L;

  public static final String STOP_PROPOSAL_PARAMETER_OBJECT_CONFIG = "stop.proposal.parameter.object";
  public static final String BOOTSTRAP_PARAMETER_OBJECT_CONFIG = "bootstrap.parameter.object";
  public static final String TRAIN_PARAMETER_OBJECT_CONFIG = "train.parameter.object";
  public static final String LOAD_PARAMETER_OBJECT_CONFIG = "load.parameter.object";
  public static final String PARTITION_LOAD_PARAMETER_OBJECT_CONFIG = "partition.load.parameter.object";
  public static final String PROPOSALS_PARAMETER_OBJECT_CONFIG = "proposals.parameter.object";
  public static final String STATE_PARAMETER_OBJECT_CONFIG = "state.parameter.object";
  public static final String KAFKA_CLUSTER_STATE_PARAMETER_OBJECT_CONFIG = "kafka.cluster.state.parameter.object";
  public static final String USER_TASKS_PARAMETER_OBJECT_CONFIG = "user.tasks.parameter.object";
  public static final String REVIEW_BOARD_PARAMETER_OBJECT_CONFIG = "review.board.parameter.object";
  public static final String ADD_BROKER_PARAMETER_OBJECT_CONFIG = "add.broker.parameter.object";
  public static final String REMOVE_BROKER_PARAMETER_OBJECT_CONFIG = "remove.broker.parameter.object";
  public static final String REBALANCE_PARAMETER_OBJECT_CONFIG = "rebalance.parameter.object";
  public static final String PAUSE_RESUME_PARAMETER_OBJECT_CONFIG = "pause.resume.parameter.object";
  public static final String DEMOTE_BROKER_PARAMETER_OBJECT_CONFIG = "demote.broker.parameter.object";
  public static final String ADMIN_PARAMETER_OBJECT_CONFIG = "admin.parameter.object";
  public static final String REVIEW_PARAMETER_OBJECT_CONFIG = "review.parameter.object";
  public static final String TOPIC_CONFIGURATION_PARAMETER_OBJECT_CONFIG = "topic.configuration.parameter.object";
  private static final Map<CruiseControlEndPoint, Set<String>> VALID_ENDPOINT_PARAM_NAMES;

  static {
    Map<CruiseControlEndPoint, Set<String>> validParamNames = new HashMap<>();

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
    load.add(START_MS_PARAM);
    load.add(END_MS_PARAM);
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
    partitionLoad.add(AVG_LOAD_PARAM);
    partitionLoad.add(TOPIC_PARAM);
    partitionLoad.add(PARTITION_PARAM);
    partitionLoad.add(MIN_VALID_PARTITION_RATIO_PARAM);
    partitionLoad.add(BROKER_ID_PARAM);

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
    proposals.add(DESTINATION_BROKER_IDS_PARAM);

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
    addOrRemoveBroker.add(REPLICA_MOVEMENT_STRATEGIES_PARAM);
    addOrRemoveBroker.add(REVIEW_ID_PARAM);

    Set<String> addBroker = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    addBroker.add(THROTTLE_ADDED_BROKER_PARAM);
    addBroker.addAll(addOrRemoveBroker);

    Set<String> removeBroker = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    removeBroker.add(THROTTLE_REMOVED_BROKER_PARAM);
    removeBroker.add(DESTINATION_BROKER_IDS_PARAM);
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
    demoteBroker.add(REPLICA_MOVEMENT_STRATEGIES_PARAM);
    demoteBroker.add(REVIEW_ID_PARAM);

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
    rebalance.add(REPLICA_MOVEMENT_STRATEGIES_PARAM);
    rebalance.add(IGNORE_PROPOSAL_CACHE_PARAM);
    rebalance.add(REVIEW_ID_PARAM);
    rebalance.add(DESTINATION_BROKER_IDS_PARAM);

    Set<String> kafkaClusterState = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    kafkaClusterState.add(VERBOSE_PARAM);
    kafkaClusterState.add(JSON_PARAM);
    kafkaClusterState.add(TOPIC_PARAM);

    Set<String> pauseSampling = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    pauseSampling.add(REASON_PARAM);
    pauseSampling.add(JSON_PARAM);
    pauseSampling.add(REVIEW_ID_PARAM);

    Set<String> resumeSampling = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    resumeSampling.add(REASON_PARAM);
    resumeSampling.add(JSON_PARAM);
    resumeSampling.add(REVIEW_ID_PARAM);

    Set<String> stopProposalExecution = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    stopProposalExecution.add(JSON_PARAM);
    stopProposalExecution.add(REVIEW_ID_PARAM);

    Set<String> userTasks = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    userTasks.add(JSON_PARAM);
    userTasks.add(USER_TASK_IDS_PARAM);
    userTasks.add(CLIENT_IDS_PARAM);
    userTasks.add(ENTRIES_PARAM);
    userTasks.add(ENDPOINTS_PARAM);
    userTasks.add(TYPES_PARAM);
    userTasks.add(FETCH_COMPLETED_TASK_PARAM);

    Set<String> admin = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    admin.add(JSON_PARAM);
    admin.add(DISABLE_SELF_HEALING_FOR_PARAM);
    admin.add(ENABLE_SELF_HEALING_FOR_PARAM);
    admin.add(CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_PARAM);
    admin.add(CONCURRENT_LEADER_MOVEMENTS_PARAM);
    admin.add(DROP_RECENTLY_REMOVED_BROKERS_PARAM);
    admin.add(DROP_RECENTLY_DEMOTED_BROKERS_PARAM);
    admin.add(REVIEW_ID_PARAM);

    Set<String> review = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    review.add(APPROVE_PARAM);
    review.add(DISCARD_PARAM);
    review.add(REASON_PARAM);
    review.add(JSON_PARAM);

    Set<String> reviewBoard = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    reviewBoard.add(JSON_PARAM);
    reviewBoard.add(REVIEW_IDS_PARAM);

    Set<String> topicConfiguration = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    topicConfiguration.add(JSON_PARAM);
    topicConfiguration.add(TOPIC_PARAM);
    topicConfiguration.add(REPLICATION_FACTOR_PARAM);
    topicConfiguration.add(SKIP_RACK_AWARENESS_CHECK_PARAM);
    topicConfiguration.add(DRY_RUN_PARAM);
    topicConfiguration.add(GOALS_PARAM);
    topicConfiguration.add(DATA_FROM_PARAM);
    topicConfiguration.add(SKIP_HARD_GOAL_CHECK_PARAM);
    topicConfiguration.add(ALLOW_CAPACITY_ESTIMATION_PARAM);
    topicConfiguration.add(CONCURRENT_LEADER_MOVEMENTS_PARAM);
    topicConfiguration.add(CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_PARAM);
    topicConfiguration.add(EXCLUDE_RECENTLY_DEMOTED_BROKERS_PARAM);
    topicConfiguration.add(EXCLUDE_RECENTLY_REMOVED_BROKERS_PARAM);
    topicConfiguration.add(REPLICA_MOVEMENT_STRATEGIES_PARAM);
    topicConfiguration.add(VERBOSE_PARAM);
    topicConfiguration.add(REVIEW_ID_PARAM);

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
    validParamNames.put(REVIEW, Collections.unmodifiableSet(review));
    validParamNames.put(REVIEW_BOARD, Collections.unmodifiableSet(reviewBoard));
    validParamNames.put(TOPIC_CONFIGURATION, Collections.unmodifiableSet(topicConfiguration));

    VALID_ENDPOINT_PARAM_NAMES = Collections.unmodifiableMap(validParamNames);
  }

  private ParameterUtils() {
  }

  public static CruiseControlEndPoint endPoint(HttpServletRequest request) {
    List<CruiseControlEndPoint> supportedEndpoints;
    switch (request.getMethod()) {
      case GET_METHOD:
        supportedEndpoints = CruiseControlEndPoint.getEndpoints();
        break;
      case POST_METHOD:
        supportedEndpoints = CruiseControlEndPoint.postEndpoints();
        break;
      default:
        throw new UserRequestException("Unsupported request method: " + request.getMethod() + ".");
    }

    String path = request.getRequestURI().toUpperCase().replace(REQUEST_URI, "");
    for (CruiseControlEndPoint endPoint : supportedEndpoints) {
      if (endPoint.toString().equalsIgnoreCase(path)) {
        return endPoint;
      }
    }
    return null;
  }

  static void handleParameterParseException(Exception e,
                                            HttpServletResponse response,
                                            String errorMsg,
                                            boolean json,
                                            KafkaCruiseControlConfig config) throws IOException {
    StringWriter sw = new StringWriter();
    e.printStackTrace(new PrintWriter(sw));
    writeErrorResponse(response, sw.toString(), errorMsg, SC_BAD_REQUEST, json, config);
  }

  public static boolean hasValidParameters(HttpServletRequest request, HttpServletResponse response, KafkaCruiseControlConfig config)
      throws IOException {
    CruiseControlEndPoint endPoint = endPoint(request);
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
      writeErrorResponse(response, "", errorResp, SC_BAD_REQUEST, wantJSON(request), config);
      return false;
    }
    return true;
  }

  /**
   * Returns the case sensitive request parameter name, or <code>null</code> if the parameter does not exist.
   */
  public static String caseSensitiveParameterName(Map<String, String[]> parameterMap, String parameter) {
    return parameterMap.keySet().stream().filter(parameter::equalsIgnoreCase).findFirst().orElse(null);
  }

  private static boolean getBooleanParam(HttpServletRequest request, String parameter, boolean defaultIfMissing) {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), parameter);
    return parameterString == null ? defaultIfMissing : Boolean.parseBoolean(request.getParameter(parameterString));
  }

  private static List<String> getListParam(HttpServletRequest request, String parameter) throws UnsupportedEncodingException {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), parameter);
    List<String> retList = parameterString == null ? new ArrayList<>()
                                                   : Arrays.asList(urlDecode(request.getParameter(parameterString)).split(","));
    retList.removeIf(String::isEmpty);
    return Collections.unmodifiableList(retList);
  }

  public static boolean wantJSON(HttpServletRequest request) {
    return getBooleanParam(request, JSON_PARAM, false);
  }

  static boolean allowCapacityEstimation(HttpServletRequest request) {
    return getBooleanParam(request, ALLOW_CAPACITY_ESTIMATION_PARAM, true);
  }

  static boolean skipRackAwarenessCheck(HttpServletRequest request) {
    return getBooleanParam(request, SKIP_RACK_AWARENESS_CHECK_PARAM, false);
  }

  private static boolean excludeBrokers(HttpServletRequest request, String parameter, boolean defaultIfMissing) {
    boolean isKafkaAssignerMode = isKafkaAssignerMode(request);
    boolean excludeBrokers = getBooleanParam(request, parameter, defaultIfMissing);
    if (isKafkaAssignerMode && excludeBrokers) {
      throw new UserRequestException("Kafka assigner mode does not support excluding brokers.");
    }

    return excludeBrokers;
  }

  private static boolean getBooleanExcludeGiven(HttpServletRequest request, String getParameter, String excludeParameter) {
    boolean booleanParam = getBooleanParam(request, getParameter, false);
    if (booleanParam && caseSensitiveParameterName(request.getParameterMap(), excludeParameter) != null) {
      throw new UserRequestException("Cannot set " + getParameter + " parameter to true when explicitly specifying "
                                     + excludeParameter + " in the request.");
    }
    return booleanParam;
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

  static boolean wantAvgLoad(HttpServletRequest request) {
    return getBooleanParam(request, AVG_LOAD_PARAM, false);
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

  private static boolean isKafkaAssignerMode(HttpServletRequest request) {
    return getBooleanParam(request, KAFKA_ASSIGNER_MODE_PARAM, false);
  }

  static boolean ignoreProposalCache(HttpServletRequest request) {
    return getBooleanExcludeGiven(request, IGNORE_PROPOSAL_CACHE_PARAM, GOALS_PARAM);
  }

  static boolean useReadyDefaultGoals(HttpServletRequest request) {
    return getBooleanExcludeGiven(request, USE_READY_DEFAULT_GOALS_PARAM, GOALS_PARAM);
  }

  static boolean getDryRun(HttpServletRequest request) {
    return getBooleanParam(request, DRY_RUN_PARAM, true);
  }

  static boolean throttleAddedOrRemovedBrokers(HttpServletRequest request, EndPoint endPoint) {
    return endPoint == ADD_BROKER ? getBooleanParam(request, THROTTLE_ADDED_BROKER_PARAM, true)
                                  : getBooleanParam(request, THROTTLE_REMOVED_BROKER_PARAM, true);
  }

  static Long time(HttpServletRequest request) {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), TIME_PARAM);
    if (parameterString == null) {
      return null;
    }

    if (caseSensitiveParameterName(request.getParameterMap(), END_MS_PARAM) != null) {
      throw new UserRequestException(String.format("Parameter %s and parameter %s are mutually exclusive and should "
                                                   + "not be specified in the same request.", TIME_PARAM, END_MS_PARAM));
    }

    String timeString = request.getParameter(parameterString);
    return timeString.toUpperCase().equals("NOW") ? System.currentTimeMillis() : Long.parseLong(timeString);
  }

  static long startMs(HttpServletRequest request) {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), START_MS_PARAM);
    return parameterString == null ? DEFAULT_START_TIME_FOR_CLUSTER_MODEL
                                   : Long.parseLong(request.getParameter(parameterString));
  }

  static long endMs(HttpServletRequest request) {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), END_MS_PARAM);
    return parameterString == null ? System.currentTimeMillis()
                                   : Long.parseLong(request.getParameter(parameterString));
  }

  static Pattern topic(HttpServletRequest request) {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), TOPIC_PARAM);
    return parameterString == null ? null : Pattern.compile(request.getParameter(parameterString));
  }

  static Double minValidPartitionRatio(HttpServletRequest request) {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), MIN_VALID_PARTITION_RATIO_PARAM);
    if (parameterString == null) {
      return null;
    } else {
      Double minValidPartitionRatio = Double.parseDouble(request.getParameter(parameterString));
      if (minValidPartitionRatio > 1.0 || minValidPartitionRatio < 0.0) {
        throw new UserRequestException("The requested minimum partition ratio must be in range [0.0, 1.0] (Requested: "
                                       + minValidPartitionRatio.toString() + ").");
      }
      return minValidPartitionRatio;
    }
  }

  static String resourceString(HttpServletRequest request) {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), RESOURCE_PARAM);
    return parameterString == null ? DEFAULT_PARTITION_LOAD_RESOURCE : request.getParameter(parameterString);
  }

  public static String reason(HttpServletRequest request) {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), REASON_PARAM);
    if (parameterString != null && parameterString.length() > MAX_REASON_LENGTH) {
      throw new UserRequestException(String.format("Reason cannot be longer than %d characters (attempted: %d).",
                                                   MAX_REASON_LENGTH, parameterString.length()));
    }
    String ip = getClientIpAddress(request);
    return String.format("%s (Client: %s, Date: %s)", parameterString == null ? "No reason provided"
                                                                              : request.getParameter(parameterString), ip, currentUtcDate());
  }

  private static Set<String> parseParamToStringSet(HttpServletRequest request, String param) throws UnsupportedEncodingException {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), param);
    Set<String> paramsString = parameterString == null
                               ? new HashSet<>(0)
                               : new HashSet<>(Arrays.asList(urlDecode(request.getParameter(parameterString)).split(",")));
    paramsString.removeIf(String::isEmpty);
    return paramsString;
  }

  private static Set<Integer> parseParamToIntegerSet(HttpServletRequest request, String param) throws UnsupportedEncodingException {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), param);

    return parameterString == null ? new HashSet<>(0)
                                   : Arrays.stream(urlDecode(request.getParameter(parameterString)).split(","))
                                           .map(Integer::parseInt).collect(Collectors.toSet());
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
      throw new UserRequestException(String.format("Unsupported substates in %s. Supported: %s",
                                                   substatesString, CruiseControlState.SubState.cachedValues()));
    }

    return Collections.unmodifiableSet(substates);
  }

  private static Set<AnomalyType> anomalyTypes(HttpServletRequest request, boolean isEnable) throws UnsupportedEncodingException {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), isEnable ? ENABLE_SELF_HEALING_FOR_PARAM
                                                                                            : DISABLE_SELF_HEALING_FOR_PARAM);
    Set<String> selfHealingForString = parameterString == null
                                       ? new HashSet<>(0)
                                       : new HashSet<>(Arrays.asList(urlDecode(request.getParameter(parameterString)).split(",")));
    selfHealingForString.removeIf(String::isEmpty);

    Set<AnomalyType> anomalyTypes = new HashSet<>(selfHealingForString.size());
    try {
      for (String shfString : selfHealingForString) {
        anomalyTypes.add(AnomalyType.valueOf(shfString.toUpperCase()));
      }
    } catch (IllegalArgumentException iae) {
      throw new UserRequestException(String.format("Unsupported anomaly types in %s. Supported: %s",
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
      throw new UserRequestException(String.format("The same anomaly cannot be specified in both disable and"
                                                   + "enable parameters. Intersection: %s.", intersection));
    }

    Map<Boolean, Set<AnomalyType>> selfHealingFor = new HashMap<>(2);
    selfHealingFor.put(true, enableSelfHealingFor);
    selfHealingFor.put(false, disableSelfHealingFor);

    return selfHealingFor;
  }

  static String urlDecode(String s) throws UnsupportedEncodingException {
    return s == null ? null : URLDecoder.decode(s, StandardCharsets.UTF_8.name());
  }

  /**
   * Get a composite replica movement strategy for {@link com.linkedin.kafka.cruisecontrol.executor.ExecutionTaskPlanner}.
   *
   * @param request The http request.
   * @param config The configuration of Cruise Control.
   * @return A composite strategy generated by chaining all the strategies specified by the http request.
   */
  static ReplicaMovementStrategy getReplicaMovementStrategy(HttpServletRequest request, KafkaCruiseControlConfig config)
      throws UnsupportedEncodingException {
    if (!getDryRun(request)) {
      return null;
    }
    List<String> strategies = getListParam(request, REPLICA_MOVEMENT_STRATEGIES_PARAM);
    if (strategies.isEmpty()) {
      return null;
    }

    List<ReplicaMovementStrategy> supportedStrategies = config.getConfiguredInstances(KafkaCruiseControlConfig.REPLICA_MOVEMENT_STRATEGIES_CONFIG,
                                                                                      ReplicaMovementStrategy.class);
    Map<String, ReplicaMovementStrategy> supportedStrategiesByName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    for (ReplicaMovementStrategy strategy : supportedStrategies) {
      supportedStrategiesByName.put(strategy.name(), strategy);
    }
    ReplicaMovementStrategy strategy = null;
    for (String strategyName : strategies) {
      if (supportedStrategiesByName.containsKey(strategyName)) {
        strategy = strategy == null ? supportedStrategiesByName.get(strategyName) : strategy.chain(supportedStrategiesByName.get(strategyName));
      } else {
        throw new UserRequestException("Strategy " + strategyName + " is not supported. Supported: " + supportedStrategiesByName.keySet());
      }
    }
    // Chain the generated composite strategy with BaseReplicaMovementStrategy in the end to ensure the returned strategy can always
    // determine the order of two tasks.
    return strategy.chain(new BaseReplicaMovementStrategy());
  }

  static List<String> getGoals(HttpServletRequest request) throws UnsupportedEncodingException {
    boolean isKafkaAssignerMode = isKafkaAssignerMode(request);
    List<String> goals = getListParam(request, GOALS_PARAM);

    // KafkaAssigner mode is assumed to use two KafkaAssigner goals, if client specifies goals in request, throw exception.
    if (isKafkaAssignerMode) {
      if (!goals.isEmpty()) {
        throw new UserRequestException("Kafka assigner mode does not support explicitly specifying goals in request.");
      }
      return Collections.unmodifiableList(Arrays.asList(KafkaAssignerEvenRackAwareGoal.class.getSimpleName(),
                                                        KafkaAssignerDiskUsageDistributionGoal.class.getSimpleName()));
    }
    return goals;
  }

  public static int entries(HttpServletRequest request) {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), ENTRIES_PARAM);
    return parameterString == null ? Integer.MAX_VALUE : Integer.parseInt(request.getParameter(parameterString));
  }

  /**
   * @param values Integer values
   * @return A set of negative integer values contained in the given set.
   */
  private static Set<Integer> getNegatives(Set<Integer> values) {
    return values.stream().filter(v -> v < 0).collect(Collectors.toCollection(() -> new HashSet<>(values.size())));
  }

  /**
   * Default: An empty set.
   */
  public static Set<Integer> reviewIds(HttpServletRequest request) throws UnsupportedEncodingException {
    Set<Integer> reviewIds = parseParamToIntegerSet(request, REVIEW_IDS_PARAM);
    Set<Integer> negativeReviewIds = getNegatives(reviewIds);
    if (!negativeReviewIds.isEmpty()) {
      throw new UserRequestException(String.format("%s cannot contain negative values (requested: %s).",
                                                   REVIEW_IDS_PARAM, negativeReviewIds));
    }

    return Collections.unmodifiableSet(reviewIds);
  }

  /**
   * Mutually exclusive with the other parameters and can only be used if two step verification is enabled.
   */
  public static Integer reviewId(HttpServletRequest request, boolean twoStepVerificationEnabled) {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), REVIEW_ID_PARAM);
    if (parameterString == null) {
      return null;
    } else if (!twoStepVerificationEnabled) {
      throw new UserRequestException(
          String.format("%s parameter is not relevant when two-step verification is disabled.", REVIEW_ID_PARAM));
    }

    Integer reviewId = Integer.parseInt(request.getParameter(parameterString));
    // Sanity check: Ensure that if a review id is provided, no other parameter is in the request.
    if (request.getParameterMap().size() != 1) {
      throw new UserRequestException(
          String.format("%s parameter must be mutually exclusive with other parameters (Request parameters: %s).",
                        REVIEW_ID_PARAM, request.getParameterMap()));
    } else if (reviewId < 0) {
      throw new UserRequestException(String.format("%s cannot be negative (requested: %d).", REVIEW_ID_PARAM, reviewId));
    }

    return reviewId;
  }

  /**
   * @param isPartitionMovement True if partition movement per broker, false if the total leader movement.
   */
  static Integer concurrentMovements(HttpServletRequest request, boolean isPartitionMovement) {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), isPartitionMovement
                                                                                   ? CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_PARAM
                                                                                   : CONCURRENT_LEADER_MOVEMENTS_PARAM);
    if (parameterString == null) {
      return null;
    }
    Integer concurrentMovementsPerBroker = Integer.parseInt(request.getParameter(parameterString));
    if (concurrentMovementsPerBroker <= 0) {
      throw new UserRequestException("The requested movement concurrency must be positive (Requested: "
                                     + concurrentMovementsPerBroker.toString() + ").");
    }

    return concurrentMovementsPerBroker;
  }

  static Pattern excludedTopics(HttpServletRequest request) {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), EXCLUDED_TOPICS_PARAM);
    return parameterString == null ? null : Pattern.compile(request.getParameter(parameterString));
  }

  /**
   * @param isUpperBound True if upper bound, false if lower bound.
   */
  static int partitionBoundary(HttpServletRequest request, boolean isUpperBound) {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), PARTITION_PARAM);
    if (parameterString == null) {
      return isUpperBound ? Integer.MAX_VALUE : Integer.MIN_VALUE;
    }
    String partitionString = request.getParameter(parameterString);
    if (!partitionString.contains("-")) {
      return Integer.parseInt(partitionString);
    }

    String[] boundaries = partitionString.split("-");
    if (boundaries.length > 2) {
      throw new UserRequestException("The " + PARTITION_PARAM + " parameter cannot contain multiple dashes.");
    }
    return Integer.parseInt(boundaries[isUpperBound ? 1 : 0]);
  }

  static Set<Integer> brokerIds(HttpServletRequest request, boolean isOptional) throws UnsupportedEncodingException {
    Set<Integer> brokerIds = parseParamToIntegerSet(request, BROKER_ID_PARAM);
    if (!isOptional && brokerIds.isEmpty()) {
      throw new UserRequestException("Target broker ID is not provided.");
    }
    return Collections.unmodifiableSet(brokerIds);
  }

  static Set<Integer> dropRecentlyRemovedBrokers(HttpServletRequest request) throws UnsupportedEncodingException {
    return Collections.unmodifiableSet(parseParamToIntegerSet(request, DROP_RECENTLY_REMOVED_BROKERS_PARAM));
  }

  static Set<Integer> dropRecentlyDemotedBrokers(HttpServletRequest request) throws UnsupportedEncodingException {
    return Collections.unmodifiableSet(parseParamToIntegerSet(request, DROP_RECENTLY_DEMOTED_BROKERS_PARAM));
  }

  /**
   * Default: An empty set.
   */
  static Set<Integer> destinationBrokerIds(HttpServletRequest request) throws UnsupportedEncodingException {
    Set<Integer> brokerIds = Collections.unmodifiableSet(parseParamToIntegerSet(request, DESTINATION_BROKER_IDS_PARAM));
    if (!brokerIds.isEmpty()) {
      if (isKafkaAssignerMode(request)) {
        throw new UserRequestException("Kafka assigner mode does not support explicitly specifying destination broker ids.");
      }

      Set<Integer> intersection = new HashSet<>(parseParamToIntegerSet(request, BROKER_ID_PARAM));
      intersection.retainAll(brokerIds);
      if (!intersection.isEmpty()) {
        throw new UserRequestException("No overlap is allowed between the specified destination broker ids and broker ids.");
      }
    }

    return brokerIds;
  }

  /**
   * Default: An empty set.
   */
  private static Set<Integer> review(HttpServletRequest request, boolean isApprove) throws UnsupportedEncodingException {
    Set<Integer> parsedReview = parseParamToIntegerSet(request, isApprove ? APPROVE_PARAM : DISCARD_PARAM);
    return Collections.unmodifiableSet(parsedReview);
  }

  /**
   * Get {@link ReviewStatus#APPROVED} and {@link ReviewStatus#DISCARDED} requests via {@link #APPROVE_PARAM} and
   * {@link #DISCARD_PARAM}.
   *
   * Sanity check ensures that the same request cannot be specified in both configs.
   */
  static Map<ReviewStatus, Set<Integer>> reviewRequests(HttpServletRequest request) throws UnsupportedEncodingException {
    Set<Integer> approve = review(request, true);
    Set<Integer> discard = review(request, false);

    // Sanity check: Ensure that the same
    Set<Integer> intersection = new HashSet<>(approve);
    intersection.retainAll(discard);
    if (!intersection.isEmpty()) {
      throw new UserRequestException(String.format("The same request cannot be specified in both approve and"
                                                   + "discard parameters. Intersection: %s.", intersection));
    } else if (approve.isEmpty() && discard.isEmpty()) {
      throw new UserRequestException(String.format("%s endpoint requires at least one of '%s' or '%s' parameter.",
                                                   REVIEW, APPROVE_PARAM, DISCARD_PARAM));
    }

    Map<ReviewStatus, Set<Integer>> reviewRequest = new HashMap<>(2);
    reviewRequest.put(APPROVED, approve);
    reviewRequest.put(DISCARDED, discard);

    return reviewRequest;
  }

  /**
   * Default: An empty set.
   */
  public static Set<UUID> userTaskIds(HttpServletRequest request) throws UnsupportedEncodingException {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), USER_TASK_IDS_PARAM);
    return parameterString == null
           ? Collections.emptySet()
           : Arrays.stream(urlDecode(request.getParameter(parameterString)).split(",")).map(UUID::fromString).collect(Collectors.toSet());
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
  public static Set<CruiseControlEndPoint> endPoints(HttpServletRequest request) throws UnsupportedEncodingException {
    Set<String> parsedEndPoints = parseParamToStringSet(request, ENDPOINTS_PARAM).stream()
                                                                                 .map(String::toUpperCase)
                                                                                 .collect(Collectors.toSet());

    Set<CruiseControlEndPoint> endPoints = new HashSet<>();
    for (CruiseControlEndPoint endPoint : CruiseControlEndPoint.cachedValues()) {
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
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), DATA_FROM_PARAM);
    if (parameterString != null) {
      dataFrom = DataFrom.valueOf(request.getParameter(parameterString).toUpperCase());
    }
    return dataFrom;
  }

  /**
   * Skip hard goal check in kafka_assigner mode.
   */
  static boolean skipHardGoalCheck(HttpServletRequest request) {
    return isKafkaAssignerMode(request) || getBooleanParam(request, SKIP_HARD_GOAL_CHECK_PARAM, false);
  }

  static boolean skipUrpDemotion(HttpServletRequest request) {
    return getBooleanParam(request, SKIP_URP_DEMOTION_PARAM, false);
  }

  static boolean excludeFollowerDemotion(HttpServletRequest request) {
    return getBooleanParam(request, EXCLUDE_FOLLOWER_DEMOTION_PARAM, false);
  }

  static short replicationFactor(HttpServletRequest request) {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), REPLICATION_FACTOR_PARAM);
    if (parameterString == null) {
      throw new UserRequestException("Topic's replication factor is not specified.");
    }
    return Short.parseShort(request.getParameter(parameterString));
  }

  static boolean fetchCompletedTask(HttpServletRequest request) {
    return getBooleanParam(request, FETCH_COMPLETED_TASK_PARAM, false);
  }

  public enum DataFrom {
    VALID_WINDOWS, VALID_PARTITIONS
  }
}
