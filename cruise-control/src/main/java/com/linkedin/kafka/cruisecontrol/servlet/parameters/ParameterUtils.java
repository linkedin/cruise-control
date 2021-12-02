/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.google.gson.Gson;
import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.cruisecontrol.servlet.EndPoint;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionRecommendation;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerEvenRackAwareGoal;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType;
import com.linkedin.kafka.cruisecontrol.executor.ConcurrencyType;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;
import com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager;
import com.linkedin.kafka.cruisecontrol.servlet.purgatory.ReviewStatus;
import com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlState;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
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
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.linkedin.cruisecontrol.CruiseControlUtils.currentUtcDate;
import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.ADD_BROKER;
import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.DEMOTE_BROKER;
import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.FIX_OFFLINE_REPLICAS;
import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.REVIEW;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.GET_METHOD;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.POST_METHOD;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.getClientIpAddress;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.TopicConfigurationParameters.TopicConfigurationType.REPLICATION_FACTOR;
import static com.linkedin.kafka.cruisecontrol.servlet.purgatory.ReviewStatus.APPROVED;
import static com.linkedin.kafka.cruisecontrol.servlet.purgatory.ReviewStatus.DISCARDED;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.writeErrorResponse;
import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST;

/**
 * The util class for Kafka Cruise Control parameters.
 */
public final class ParameterUtils {
  public static final String JSON_PARAM = "json";
  public static final String GET_RESPONSE_SCHEMA = "get_response_schema";
  public static final String START_MS_PARAM = "start";
  public static final String END_MS_PARAM = "end";
  public static final String ENTRIES_PARAM = "entries";
  public static final String ALLOW_CAPACITY_ESTIMATION_PARAM = "allow_capacity_estimation";
  public static final String STOP_ONGOING_EXECUTION_PARAM = "stop_ongoing_execution";
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
  public static final String REPLICATION_THROTTLE_PARAM = "replication_throttle";
  public static final String IGNORE_PROPOSAL_CACHE_PARAM = "ignore_proposal_cache";
  public static final String USE_READY_DEFAULT_GOALS_PARAM = "use_ready_default_goals";
  public static final String EXECUTION_PROGRESS_CHECK_INTERVAL_MS_PARAM = "execution_progress_check_interval_ms";
  public static final String CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_PARAM = "concurrent_partition_movements_per_broker";
  public static final String MAX_PARTITION_MOVEMENTS_IN_CLUSTER_PARAM = "max_partition_movements_in_cluster";
  public static final String CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_PARAM = "concurrent_intra_broker_partition_movements";
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
  public static final String DISABLE_CONCURRENCY_ADJUSTER_FOR_PARAM = "disable_concurrency_adjuster_for";
  public static final String ENABLE_CONCURRENCY_ADJUSTER_FOR_PARAM = "enable_concurrency_adjuster_for";
  public static final String MIN_ISR_BASED_CONCURRENCY_ADJUSTMENT_PARAM = "min_isr_based_concurrency_adjustment";
  public static final String EXCLUDE_RECENTLY_DEMOTED_BROKERS_PARAM = "exclude_recently_demoted_brokers";
  public static final String EXCLUDE_RECENTLY_REMOVED_BROKERS_PARAM = "exclude_recently_removed_brokers";
  public static final String REPLICA_MOVEMENT_STRATEGIES_PARAM = "replica_movement_strategies";
  public static final String APPROVE_PARAM = "approve";
  public static final String DISCARD_PARAM = "discard";
  public static final String REBALANCE_DISK_MODE_PARAM = "rebalance_disk";
  public static final String POPULATE_DISK_INFO_PARAM = "populate_disk_info";
  public static final String CAPACITY_ONLY_PARAM = "capacity_only";
  public static final String BROKER_ID_AND_LOGDIRS_PARAM = "brokerid_and_logdirs";
  public static final String REPLICATION_FACTOR_PARAM = "replication_factor";
  public static final String SKIP_RACK_AWARENESS_CHECK_PARAM = "skip_rack_awareness_check";
  public static final String FETCH_COMPLETED_TASK_PARAM = "fetch_completed_task";
  public static final String FORCE_STOP_PARAM = "force_stop";
  public static final String FAST_MODE_PARAM = "fast_mode";
  public static final String STOP_EXTERNAL_AGENT_PARAM = "stop_external_agent";
  public static final String DEVELOPER_MODE_PARAM = "developer_mode";
  private static final int MAX_REASON_LENGTH = 50;
  private static final String DELIMITER_BETWEEN_BROKER_ID_AND_LOGDIR = "-";
  public static final long DEFAULT_START_TIME_FOR_CLUSTER_MODEL = -1L;
  public static final String TOPIC_BY_REPLICATION_FACTOR = "topic_by_replication_factor";
  public static final String NO_REASON_PROVIDED = "No reason provided";
  public static final String DO_AS = "doAs";
  public static final String NUM_BROKERS_TO_ADD = "num_brokers_to_add";
  public static final String PARTITION_COUNT = "partition_count";

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
  public static final String FIX_OFFLINE_REPLICAS_PARAMETER_OBJECT_CONFIG = "fix.offline.replicas.parameter.object";
  public static final String REBALANCE_PARAMETER_OBJECT_CONFIG = "rebalance.parameter.object";
  public static final String PAUSE_RESUME_PARAMETER_OBJECT_CONFIG = "pause.resume.parameter.object";
  public static final String DEMOTE_BROKER_PARAMETER_OBJECT_CONFIG = "demote.broker.parameter.object";
  public static final String ADMIN_PARAMETER_OBJECT_CONFIG = "admin.parameter.object";
  public static final String REVIEW_PARAMETER_OBJECT_CONFIG = "review.parameter.object";
  public static final String TOPIC_CONFIGURATION_PARAMETER_OBJECT_CONFIG = "topic.configuration.parameter.object";
  public static final String RIGHTSIZE_PARAMETER_OBJECT_CONFIG = "rightsize.parameter.object";

  private ParameterUtils() {
  }

  /**
   * @param request The Http request.
   * @return The endpoint specified in the given request.
   */
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
    String pathInfo = request.getPathInfo();
    if (pathInfo == null) {
      // URL does not have any extra path information
      return null;
    }
    // Skip the first character '/'
    String path = pathInfo.substring(1);
    for (CruiseControlEndPoint endPoint : supportedEndpoints) {
      if (endPoint.toString().equalsIgnoreCase(path)) {
        return endPoint;
      }
    }
    return null;
  }

  static void handleParameterParseException(Exception e,
                                            HttpServletResponse response,
                                            String errorMessage,
                                            boolean json,
                                            boolean wantJsonSchema,
                                            KafkaCruiseControlConfig config) throws IOException {
    writeErrorResponse(response, e, errorMessage, SC_BAD_REQUEST, json, wantJsonSchema, config);
  }

  /**
   * Check whether the request has valid parameter names. If not, populate the HTTP response with the corresponding
   * error message and return {@code false}, return {@code true} otherwise.
   *
   * @param request HTTP request received by Cruise Control.
   * @param response HTTP response of Cruise Control. Populated in case of an error.
   * @param config The configurations for Cruise Control.
   * @param parameters Request parameters
   * @return {@code true} if the request has valid parameter names, {@code false} otherwise (and response is populated).
   */
  public static boolean hasValidParameterNames(HttpServletRequest request,
                                               HttpServletResponse response,
                                               KafkaCruiseControlConfig config,
                                               CruiseControlParameters parameters) throws IOException {
    CruiseControlEndPoint endPoint = endPoint(request);
    Set<String> validParamNames = parameters.caseInsensitiveParameterNames();
    Set<String> userParams = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    userParams.addAll(request.getParameterMap().keySet());
    if (validParamNames != null) {
      userParams.removeAll(validParamNames);
    }

    if (!userParams.isEmpty()) {
      // User request specifies parameters that are not a subset of the valid parameters.
      String errorMessage = String.format("Unrecognized endpoint parameters in %s %s request: %s.",
                                          endPoint, request.getMethod(), userParams);
      writeErrorResponse(response, null, errorMessage, SC_BAD_REQUEST, wantJSON(request), wantResponseSchema(request), config);
      return false;
    }
    return true;
  }

  /**
   * @param parameterMap Parameter map
   * @param parameter Parameter to parse from the parameter map.
   * @return The case sensitive request parameter name, or <code>null</code> if the parameter does not exist.
   */
  public static String caseSensitiveParameterName(Map<String, String[]> parameterMap, String parameter) {
    return parameterMap.keySet().stream().filter(parameter::equalsIgnoreCase).findFirst().orElse(null);
  }

  /**
   * Get the boolean parameter.
   *
   * @param request HTTP request received by Cruise Control.
   * @param parameter Parameter to parse from the request.
   * @param defaultIfMissing Default value to set if the request does not contain the parameter.
   * @return The specified value for the parameter, or defaultIfMissing if the parameter is missing.
   */
  public static boolean getBooleanParam(HttpServletRequest request, String parameter, boolean defaultIfMissing) {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), parameter);
    return parameterString == null ? defaultIfMissing : Boolean.parseBoolean(request.getParameter(parameterString));
  }

  /**
   * Get the long parameter parameter.
   *
   * @param request HTTP request received by Cruise Control.
   * @param parameter Parameter to parse from the request.
   * @param defaultIfMissing Default value to set if the request does not contain the parameter.
   * @return The specified value for the parameter, or defaultIfMissing if the parameter is missing.
   */
  public static Long getLongParam(HttpServletRequest request, String parameter, @Nullable Long defaultIfMissing) {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), parameter);
    return parameterString == null ? defaultIfMissing : Long.valueOf(request.getParameter(parameterString));
  }

  /**
   * Get the {@link List} parameter.
   *
   * @param request HTTP request received by Cruise Control.
   * @param parameter Parameter to parse from the request.
   * @return The specified value for the parameter, or empty List if the parameter is missing.
   */
  public static List<String> getListParam(HttpServletRequest request, String parameter) throws UnsupportedEncodingException {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), parameter);
    List<String> retList = parameterString == null ? new ArrayList<>()
                                                   : Arrays.asList(urlDecode(request.getParameter(parameterString)).split(","));
    retList.removeIf(String::isEmpty);
    return Collections.unmodifiableList(retList);
  }

  public static boolean wantJSON(HttpServletRequest request) {
    return getBooleanParam(request, JSON_PARAM, false);
  }

  public static boolean wantResponseSchema(HttpServletRequest request) {
    return getBooleanParam(request, GET_RESPONSE_SCHEMA, false);
  }

  static boolean allowCapacityEstimation(HttpServletRequest request) {
    return getBooleanParam(request, ALLOW_CAPACITY_ESTIMATION_PARAM, true);
  }

  static boolean skipRackAwarenessCheck(HttpServletRequest request) {
    return getBooleanParam(request, SKIP_RACK_AWARENESS_CHECK_PARAM, false);
  }

  static boolean stopOngoingExecution(HttpServletRequest request) {
    return getBooleanParam(request, STOP_ONGOING_EXECUTION_PARAM, false);
  }

  private static boolean excludeBrokers(HttpServletRequest request, String parameter, boolean defaultIfMissing) {
    boolean isKafkaAssignerMode = isKafkaAssignerMode(request);
    boolean excludeBrokers = getBooleanParam(request, parameter, defaultIfMissing);
    if (isKafkaAssignerMode && excludeBrokers) {
      throw new UserRequestException("Kafka assigner mode does not support excluding brokers.");
    }

    return excludeBrokers;
  }

  private static boolean getBooleanExcludeGiven(HttpServletRequest request, String getParameter, Set<String> excludeParameters) {
    boolean booleanParam = getBooleanParam(request, getParameter, false);
    if (booleanParam) {
      for (String excludeParameter : excludeParameters) {
        if (caseSensitiveParameterName(request.getParameterMap(), excludeParameter) != null) {
          throw new UserRequestException("Cannot set " + getParameter + " parameter to true when explicitly specifying "
                                         + excludeParameter + " in the request.");
        }
      }
    }
    return booleanParam;
  }

  /**
   * Default: {@code false} -- i.e. recently demoted brokers may receive leadership from the other brokers as long as
   * {@link ExecutorConfig#DEMOTION_HISTORY_RETENTION_TIME_MS_CONFIG}.
   * @param request Http servlet request.
   * @return The value of {@link #EXCLUDE_RECENTLY_DEMOTED_BROKERS_PARAM} parameter.
   */
  static boolean excludeRecentlyDemotedBrokers(HttpServletRequest request) {
    return excludeBrokers(request, EXCLUDE_RECENTLY_DEMOTED_BROKERS_PARAM, false);
  }

  /**
   * Default: {@code true} -- i.e. recently removed brokers may not receive replicas from the other brokers as long as
   * {@link ExecutorConfig#REMOVAL_HISTORY_RETENTION_TIME_MS_CONFIG}.
   * @param request Http servlet request.
   * @return The value of {@link #EXCLUDE_RECENTLY_REMOVED_BROKERS_PARAM} parameter.
   */
  static boolean excludeRecentlyRemovedBrokers(HttpServletRequest request) {
    return excludeBrokers(request, EXCLUDE_RECENTLY_REMOVED_BROKERS_PARAM, true);
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

  static boolean isRebalanceDiskMode(HttpServletRequest request) {
    return getBooleanParam(request, REBALANCE_DISK_MODE_PARAM, false);
  }

  static boolean populateDiskInfo(HttpServletRequest request) {
    return getBooleanParam(request, POPULATE_DISK_INFO_PARAM, false);
  }

  static boolean capacityOnly(HttpServletRequest request) {
    Set<String> excludeParameters =
        Set.of(TIME_PARAM, END_MS_PARAM, START_MS_PARAM, ALLOW_CAPACITY_ESTIMATION_PARAM, POPULATE_DISK_INFO_PARAM);
    return getBooleanExcludeGiven(request, CAPACITY_ONLY_PARAM, excludeParameters);
  }

  static boolean ignoreProposalCache(HttpServletRequest request) {
    return getBooleanExcludeGiven(request, IGNORE_PROPOSAL_CACHE_PARAM, Collections.singleton(GOALS_PARAM));
  }

  static boolean useReadyDefaultGoals(HttpServletRequest request) {
    return getBooleanExcludeGiven(request, USE_READY_DEFAULT_GOALS_PARAM, Collections.singleton(GOALS_PARAM));
  }

  static boolean fastMode(HttpServletRequest request) {
    return getBooleanParam(request, FAST_MODE_PARAM, true);
  }

  static boolean getDryRun(HttpServletRequest request) {
    return getBooleanParam(request, DRY_RUN_PARAM, true);
  }

  static boolean forceExecutionStop(HttpServletRequest request) {
    return getBooleanParam(request, FORCE_STOP_PARAM, false);
  }

  static boolean stopExternalAgent(HttpServletRequest request) {
    return getBooleanParam(request, STOP_EXTERNAL_AGENT_PARAM, true);
  }

  static boolean developerMode(HttpServletRequest request) {
    return getBooleanParam(request, DEVELOPER_MODE_PARAM, false);
  }

  static boolean throttleAddedOrRemovedBrokers(HttpServletRequest request, EndPoint endPoint) {
    return endPoint == ADD_BROKER ? getBooleanParam(request, THROTTLE_ADDED_BROKER_PARAM, true)
                                  : getBooleanParam(request, THROTTLE_REMOVED_BROKER_PARAM, true);
  }

  static Long replicationThrottle(HttpServletRequest request, KafkaCruiseControlConfig config) {
    Long value = getLongParam(request, REPLICATION_THROTTLE_PARAM, config.getLong(ExecutorConfig.DEFAULT_REPLICATION_THROTTLE_CONFIG));
    if (value != null && value < 0) {
      throw new UserRequestException(String.format("Requested rebalance throttle must be non-negative (Requested: %s).", value));
    }
    return value;
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
    return "NOW".equalsIgnoreCase(timeString) ? System.currentTimeMillis() : Long.parseLong(timeString);
  }

  static Long startMsOrDefault(HttpServletRequest request, @Nullable Long defaultIfMissing) {
    return getLongParam(request, START_MS_PARAM, defaultIfMissing);
  }

  static Long endMsOrDefault(HttpServletRequest request, @Nullable Long defaultIfMissing) {
    return getLongParam(request, END_MS_PARAM, defaultIfMissing);
  }

  static void validateTimeRange(long startMs, long endMs) {
    if (startMs >= endMs) {
      throw new UserRequestException(
          String.format("Invalid time range. Start time must be smaller than end time. Got: [%d, %d]", startMs, endMs));
    }
  }

  static Pattern topic(HttpServletRequest request) {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), TOPIC_PARAM);
    return parameterString == null ? null : Pattern.compile(request.getParameter(parameterString));
  }

  @SuppressWarnings("unchecked")
  private static Map<Short, Pattern> topicPatternByReplicationFactorFromBody(HttpServletRequest request) {
    Map<Short, Pattern> topicPatternByReplicationFactor;
    try {
      Gson gson = new Gson();
      Map<String, Object> json = gson.fromJson(request.getReader(), Map.class);
      if (json == null) {
        return null;
      }
      String replicationFactorKey = REPLICATION_FACTOR.name().toLowerCase();
      if (!json.containsKey(replicationFactorKey)) {
        return null;
      }
      Map<String, Object> replicationFactorParams = (Map<String, Object>) json.get(replicationFactorKey);
      if (!replicationFactorParams.containsKey(TOPIC_BY_REPLICATION_FACTOR)) {
        return null;
      }
      topicPatternByReplicationFactor = new HashMap<>();
      for (Map.Entry<String, String> entry : ((Map<String, String>) replicationFactorParams.get(TOPIC_BY_REPLICATION_FACTOR)).entrySet()) {
        short replicationFactor = Short.parseShort(entry.getKey().trim());
        Pattern topicPattern = Pattern.compile(entry.getValue().trim());
        topicPatternByReplicationFactor.putIfAbsent(replicationFactor, topicPattern);
      }
    } catch (IOException ioe) {
      throw new UserRequestException(String.format("Illegal value for field %s in body, please specify in pairs of \"target replication "
                                                   + "factor\" : \"topic name regex\".", TOPIC_BY_REPLICATION_FACTOR));
    }
    return topicPatternByReplicationFactor;
  }

    static Map<Short, Pattern> topicPatternByReplicationFactor(HttpServletRequest request) {
    Pattern topic = topic(request);
    Short replicationFactor = replicationFactor(request);
    Map<Short, Pattern> topicPatternByReplicationFactorFromBody = topicPatternByReplicationFactorFromBody(request);
    if (topicPatternByReplicationFactorFromBody != null) {
      if (topic != null || replicationFactor != null) {
        throw new UserRequestException("Requesting topic replication factor change from both HTTP request parameter and body"
                                       + " is forbidden.");
      }
      return topicPatternByReplicationFactorFromBody;
    } else {
      if (topic == null && replicationFactor != null) {
        throw new UserRequestException("Topic is not specified in URL while target replication factor is specified.");
      }
      if ((topic != null && replicationFactor == null)) {
        throw new UserRequestException("Topic's replication factor is not specified in URL while subject topic is specified.");
      }
      if (topic != null) {
        return Collections.singletonMap(replicationFactor, topic);
      }
    }
    return Collections.emptyMap();
  }

  static Double minValidPartitionRatio(HttpServletRequest request) {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), MIN_VALID_PARTITION_RATIO_PARAM);
    if (parameterString == null) {
      return null;
    } else {
      double minValidPartitionRatio = Double.parseDouble(request.getParameter(parameterString));
      if (minValidPartitionRatio > 1.0 || minValidPartitionRatio < 0.0) {
        throw new UserRequestException("The requested minimum partition ratio must be in range [0.0, 1.0] (Requested: "
                                       + minValidPartitionRatio + ").");
      }
      return minValidPartitionRatio;
    }
  }

  /**
   * Get the {@link #RESOURCE_PARAM} from the request.
   *
   * @param request HTTP request received by Cruise Control.
   * @return The resource String from the request, or {@link #DEFAULT_PARTITION_LOAD_RESOURCE} if {@link #RESOURCE_PARAM}
   * does not exist in the request.
   */
  public static String resourceString(HttpServletRequest request) {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), RESOURCE_PARAM);
    return parameterString == null ? DEFAULT_PARTITION_LOAD_RESOURCE : request.getParameter(parameterString);
  }

  /**
   * Get the {@link #REASON_PARAM} from the request.
   *
   * @param request HTTP request received by Cruise Control.
   * @param reasonRequired {@code true} if the {@link #REASON_PARAM} parameter is required, {@code false} otherwise.
   * @return The specified value for the {@link #REASON_PARAM} parameter, or {@link #NO_REASON_PROVIDED} if parameter
   * does not exist in the request.
   */
  public static String reason(HttpServletRequest request, boolean reasonRequired) {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), REASON_PARAM);
    if (parameterString != null && parameterString.length() > MAX_REASON_LENGTH) {
      throw new UserRequestException(String.format("Reason cannot be longer than %d characters (attempted: %d).",
                                                   MAX_REASON_LENGTH, parameterString.length()));
    }
    String ip = getClientIpAddress(request);
    if (parameterString == null && reasonRequired) {
      throw new UserRequestException("Reason is missing in request.");
    }
    return String.format("%s (Client: %s, Date: %s)", parameterString == null ? NO_REASON_PROVIDED
                                                                              : request.getParameter(parameterString), ip, currentUtcDate());
  }

  /**
   * Parse the given parameter to a Set of String.
   *
   * @param request HTTP request received by Cruise Control.
   * @param param The name of parameter.
   * @return Parsed parameter as a Set of String.
   */
  public static Set<String> parseParamToStringSet(HttpServletRequest request, String param) throws UnsupportedEncodingException {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), param);
    Set<String> paramsString = parameterString == null
                               ? new HashSet<>(0)
                               : new HashSet<>(Arrays.asList(urlDecode(request.getParameter(parameterString)).split(",")));
    paramsString.removeIf(String::isEmpty);
    return paramsString;
  }

  /**
   * Parse the given parameter to a Set of Integer.
   *
   * @param request HTTP request received by Cruise Control.
   * @param param The name of parameter.
   * @return Parsed parameter as a Set of Integer.
   */
  public static Set<Integer> parseParamToIntegerSet(HttpServletRequest request, String param) throws UnsupportedEncodingException {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), param);

    return parameterString == null ? new HashSet<>(0)
                                   : Arrays.stream(urlDecode(request.getParameter(parameterString)).split(","))
                                           .map(Integer::parseInt).collect(Collectors.toSet());
  }

  /**
   * Empty parameter means all substates are requested.
   * @param request Http servlet request.
   * @return The value of {@link #SUBSTATES_PARAM} parameter.
   */
  static Set<CruiseControlState.SubState> substates(HttpServletRequest request) throws UnsupportedEncodingException {
    Set<String> substatesString = parseParamToStringSet(request, SUBSTATES_PARAM);

    Set<CruiseControlState.SubState> substates = new HashSet<>();
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
    Set<String> selfHealingForString = parseParamToStringSet(request, isEnable ? ENABLE_SELF_HEALING_FOR_PARAM
                                                                               : DISABLE_SELF_HEALING_FOR_PARAM);

    Set<AnomalyType> anomalyTypes = new HashSet<>();
    try {
      for (String shfString : selfHealingForString) {
        anomalyTypes.add(KafkaAnomalyType.valueOf(shfString.toUpperCase()));
      }
    } catch (IllegalArgumentException iae) {
      throw new UserRequestException(String.format("Unsupported anomaly types in %s. Supported: %s",
                                                   selfHealingForString, KafkaAnomalyType.cachedValues()));
    }

    return Collections.unmodifiableSet(anomalyTypes);
  }

  private static Set<ConcurrencyType> concurrencyTypes(HttpServletRequest request, boolean isEnable) throws UnsupportedEncodingException {
    Set<String> concurrencyForStringSet = parseParamToStringSet(request, isEnable ? ENABLE_CONCURRENCY_ADJUSTER_FOR_PARAM
                                                                                  : DISABLE_CONCURRENCY_ADJUSTER_FOR_PARAM);

    Set<ConcurrencyType> concurrencyTypes = new HashSet<>();
    try {
      for (String concurrencyForString : concurrencyForStringSet) {
        concurrencyTypes.add(ConcurrencyType.valueOf(concurrencyForString.toUpperCase()));
      }
    } catch (IllegalArgumentException iae) {
      throw new UserRequestException(String.format("Unsupported concurrency types in %s. Supported: %s",
                                                   concurrencyForStringSet, ConcurrencyType.cachedValues()));
    }

    return Collections.unmodifiableSet(concurrencyTypes);
  }

  /**
   * Get self healing types for {@link #ENABLE_SELF_HEALING_FOR_PARAM} and {@link #DISABLE_SELF_HEALING_FOR_PARAM}.
   *
   * Sanity check ensures that the same anomaly is not specified in both configs at the same request.
   * @param request Http servlet request.
   * @return The self healing types for {@link #ENABLE_SELF_HEALING_FOR_PARAM} and {@link #DISABLE_SELF_HEALING_FOR_PARAM}.
   */
  static Map<Boolean, Set<AnomalyType>> selfHealingFor(HttpServletRequest request) throws UnsupportedEncodingException {
    Set<AnomalyType> enableSelfHealingFor = anomalyTypes(request, true);
    Set<AnomalyType> disableSelfHealingFor = anomalyTypes(request, false);

    // Sanity check: Ensure that the same anomaly is not specified in both configs at the same request.
    ensureDisjoint(enableSelfHealingFor, disableSelfHealingFor,
                   "The same anomaly cannot be specified in both disable and enable parameters");

    Map<Boolean, Set<AnomalyType>> selfHealingFor = Map.of(true, enableSelfHealingFor, false, disableSelfHealingFor);
    return selfHealingFor;
  }

  /**
   * Get concurrency adjuster types for {@link #ENABLE_CONCURRENCY_ADJUSTER_FOR_PARAM} and {@link #DISABLE_CONCURRENCY_ADJUSTER_FOR_PARAM}.
   *
   * Sanity check ensures that the same concurrency type is not specified in both configs at the same request.
   * @param request Http servlet request.
   * @return The concurrency adjuster types for {@link #ENABLE_CONCURRENCY_ADJUSTER_FOR_PARAM} and {@link #DISABLE_CONCURRENCY_ADJUSTER_FOR_PARAM}.
   */
  static Map<Boolean, Set<ConcurrencyType>> concurrencyAdjusterFor(HttpServletRequest request) throws UnsupportedEncodingException {
    Set<ConcurrencyType> enableConcurrencyAdjusterFor = concurrencyTypes(request, true);
    Set<ConcurrencyType> disableConcurrencyAdjusterFor = concurrencyTypes(request, false);

    // Sanity check: Ensure that the same concurrency type is not specified in both configs at the same request.
    ensureDisjoint(enableConcurrencyAdjusterFor, disableConcurrencyAdjusterFor,
                   "The same concurrency type cannot be specified in both disable and enable parameters");

    Map<Boolean, Set<ConcurrencyType>> concurrencyAdjusterFor = Map.of(true, enableConcurrencyAdjusterFor,
                                                                       false, disableConcurrencyAdjusterFor);
    return concurrencyAdjusterFor;
  }

  /**
   * @param request The http request.
   * @return {@code true}: enable or {@code false}: disable MinISR-based concurrency adjustment, {@code null} if the request parameter is unset.
   */
  @Nullable
  static Boolean minIsrBasedConcurrencyAdjustment(HttpServletRequest request) {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), MIN_ISR_BASED_CONCURRENCY_ADJUSTMENT_PARAM);
    if (parameterString == null) {
      return null;
    }
    return Boolean.parseBoolean(request.getParameter(parameterString));
  }

  /**
   * Compare and ensure two sets are disjoint as part of a user request.
   * @param set1 The first set to compare.
   * @param set2 The second set to compare.
   * @param errorMessage The message to pass to {@link UserRequestException}if two sets are not disjoint.
   * @param <E> The type of elements maintained by the sets.
   */
  static <E> void ensureDisjoint(Set<E> set1, Set<E> set2, String errorMessage) {
    Set<E> intersection = new HashSet<>(set1);
    intersection.retainAll(set2);
    if (!intersection.isEmpty()) {
      throw new UserRequestException(String.format("%s. Intersection: %s.", errorMessage, intersection));
    }
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
    if (getDryRun(request)) {
      return null;
    }
    List<String> strategies = getListParam(request, REPLICA_MOVEMENT_STRATEGIES_PARAM);
    if (strategies.isEmpty()) {
      return null;
    }

    List<ReplicaMovementStrategy> supportedStrategies = config.getConfiguredInstances(ExecutorConfig.REPLICA_MOVEMENT_STRATEGIES_CONFIG,
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
    return strategy.chainBaseReplicaMovementStrategyIfAbsent();
  }

  static List<String> getGoals(HttpServletRequest request) throws UnsupportedEncodingException {
    boolean isKafkaAssignerMode = isKafkaAssignerMode(request);
    boolean isRebalanceDiskMode = isRebalanceDiskMode(request);
    // Sanity check isKafkaAssignerMode and isRebalanceDiskMode are not both true at the same time.
    if (isKafkaAssignerMode && isRebalanceDiskMode) {
      throw new UserRequestException("Kafka assigner mode and rebalance disk mode cannot be set the at the same time.");
    }
    List<String> goals = getListParam(request, GOALS_PARAM);

    // KafkaAssigner mode is assumed to use two KafkaAssigner goals, if client specifies goals in request, throw exception.
    if (isKafkaAssignerMode) {
      if (!goals.isEmpty()) {
        throw new UserRequestException("Kafka assigner mode does not support explicitly specifying goals in request.");
      }
      return List.of(KafkaAssignerEvenRackAwareGoal.class.getSimpleName(), KafkaAssignerDiskUsageDistributionGoal.class.getSimpleName());
    }
    if (isRebalanceDiskMode) {
      if (!goals.isEmpty()) {
        throw new UserRequestException("Rebalance disk mode does not support explicitly specifying goals in request.");
      }
      return List.of(IntraBrokerDiskCapacityGoal.class.getSimpleName(), IntraBrokerDiskUsageDistributionGoal.class.getSimpleName());
    }
    return goals;
  }

  /**
   * Get the specified value for the {@link #ENTRIES_PARAM} parameter.
   *
   * @param request HTTP request received by Cruise Control.
   * @return The specified value for the {@link #ENTRIES_PARAM} parameter, or {@link Integer#MAX_VALUE} if the
   * parameter is missing.
   */
  public static int entries(HttpServletRequest request) {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), ENTRIES_PARAM);
    int entries = parameterString == null ? Integer.MAX_VALUE : Integer.parseInt(request.getParameter(parameterString));
    if (entries <= 0) {
      throw new UserRequestException("The requested entries must be positive (Requested: " + entries + ").");
    }
    return entries;
  }

  /**
   * @param values Integer values
   * @return A set of negative integer values contained in the given set.
   */
  private static Set<Integer> getNegatives(Set<Integer> values) {
    return values.stream().filter(v -> v < 0).collect(Collectors.toCollection(() -> new HashSet<>()));
  }

  /**
   * Default: An empty set.
   * @param request Http servlet request.
   * @return Review Ids.
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
   * @param request Http servlet request.
   * @param twoStepVerificationEnabled {@code true} if two-step verification is enabled, {@code false} otherwise.
   * @return Review Id.
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
   * Get the execution progress check interval in milliseconds. Default: {@code null}.
   *
   * To prevent setting this value to a very small value by mistake, the Executor ensures that the requested execution
   * progress check interval is not smaller than the configured minimum limit
   * (see {@link com.linkedin.kafka.cruisecontrol.executor.Executor#setRequestedExecutionProgressCheckIntervalMs}).
   *
   * @param request The Http request.
   * @return Execution progress check interval in milliseconds.
   */
  static Long executionProgressCheckIntervalMs(HttpServletRequest request) {
    return getLongParam(request, EXECUTION_PROGRESS_CHECK_INTERVAL_MS_PARAM, null);
  }

  /**
   * Get the execution concurrency requirement dynamically set from the Http request.
   * Based on value of isInterBrokerPartitionMovement and isIntraBrokerPartitionMovement, different type of concurrency
   * requirement is set. The mapping is as follows.
   * <ul>
   *   <li>{false, false} -> leader movement concurrency requirement</li>
   *   <li>{true , false} -> per-broker inter-broker partition movement concurrency requirement</li>
   *   <li>{false, true}  -> intra-broker partition movement concurrency requirement</li>
   *   <li>{true , true}  -> not defined</li>
   * </ul>
   *
   * @param request                        The Http request.
   * @param isInterBrokerPartitionMovement {@code true} if inter-broker partition movement per broker.
   * @param isIntraBrokerPartitionMovement {@code true} if intra-broker partition movement.
   * @return The execution concurrency requirement dynamically set from the Http request.
   */
  static Integer concurrentMovements(HttpServletRequest request,
                                     boolean isInterBrokerPartitionMovement,
                                     boolean isIntraBrokerPartitionMovement) {
    String parameter = isInterBrokerPartitionMovement
                       ? CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_PARAM
                       : isIntraBrokerPartitionMovement ? CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_PARAM : CONCURRENT_LEADER_MOVEMENTS_PARAM;
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), parameter);
    if (parameterString == null) {
      return null;
    }
    int concurrentMovementsPerBroker = Integer.parseInt(request.getParameter(parameterString));
    if (concurrentMovementsPerBroker <= 0) {
      throw new UserRequestException("The requested movement concurrency must be positive (Requested: " + concurrentMovementsPerBroker + ").");
    }

    return concurrentMovementsPerBroker;
  }

  /**
   * Get the max inter broker partition movements requirement dynamically set from the Http request.
   *
   * @param request The HTTP Request
   * @return The execution upper bound requirement dynamically set from the Http request.
   */
  static Integer maxPartitionMovements(HttpServletRequest request) {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), MAX_PARTITION_MOVEMENTS_IN_CLUSTER_PARAM);
    if (parameterString == null) {
      return null;
    }
    int maxPartitionMovements = Integer.parseInt(request.getParameter(parameterString));
    if (maxPartitionMovements <= 0) {
      throw new UserRequestException("The requested max partition movement must be positive (Requested: " + maxPartitionMovements + ").");
    }
    return maxPartitionMovements;
  }

  static Pattern excludedTopics(HttpServletRequest request) {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), EXCLUDED_TOPICS_PARAM);
    return parameterString == null ? null : Pattern.compile(request.getParameter(parameterString));
  }

  /**
   * Default: {@link Integer#MAX_VALUE} for upper bound parameter, or {@link Integer#MIN_VALUE} otherwise.
   * @param request Http servlet request.
   * @param isUpperBound {@code true} if upper bound, {@code false} if lower bound.
   * @return The value of {@link #PARTITION_PARAM} parameter.
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

  /**
   * Get the {@link #NUM_BROKERS_TO_ADD} from the request.
   *
   * Default: {@link ProvisionRecommendation#DEFAULT_OPTIONAL_INT}
   * @param request Http servlet request.
   * @return The value of {@link #NUM_BROKERS_TO_ADD} parameter.
   * @throws UserRequestException if the number of brokers to add is not a positive integer.
   */
  static int numBrokersToAdd(HttpServletRequest request) {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), NUM_BROKERS_TO_ADD);
    if (parameterString == null) {
      return ProvisionRecommendation.DEFAULT_OPTIONAL_INT;
    }
    int numBrokersToAdd = Integer.parseInt(request.getParameter(parameterString));
    if (numBrokersToAdd <= 0) {
      throw new UserRequestException("The requested number of brokers to add must be positive (Requested: " + numBrokersToAdd + ").");
    }
    return numBrokersToAdd;
  }

  /**
   * Get the {@link #PARTITION_COUNT} from the request.
   *
   * Default: {@link ProvisionRecommendation#DEFAULT_OPTIONAL_INT}
   * @param request Http servlet request.
   * @return The value of {@link #PARTITION_COUNT} parameter.
   * @throws UserRequestException if the targeted partition count is not a positive integer.
   */
  static int partitionCount(HttpServletRequest request) {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), PARTITION_COUNT);
    if (parameterString == null) {
      return ProvisionRecommendation.DEFAULT_OPTIONAL_INT;
    }
    int targetPartitionCount = Integer.parseInt(request.getParameter(parameterString));
    if (targetPartitionCount <= 0) {
      throw new UserRequestException("The requested targeted partition count must be positive (Requested: " + targetPartitionCount + ").");
    }
    return targetPartitionCount;
  }

  static Set<Integer> brokerIds(HttpServletRequest request, boolean isOptional) throws UnsupportedEncodingException {
    Set<Integer> brokerIds = parseParamToIntegerSet(request, BROKER_ID_PARAM);
    if (!isOptional && brokerIds.isEmpty()) {
      EndPoint endpoint = endPoint(request);
      if (endpoint == DEMOTE_BROKER) {
        // If it is a demote_broker request, either target broker or target disk should be specified in request.
        if (brokerIdAndLogdirs(request).isEmpty()) {
          throw new UserRequestException("No target broker ID or target disk logdir is specified to demote.");
        }
      } else if (endpoint != FIX_OFFLINE_REPLICAS) {
        throw new UserRequestException(String.format("Target broker ID is not provided for %s request", endpoint));
      }
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
   * @param request Http servlet request.
   * @return The value of {@link #DESTINATION_BROKER_IDS_PARAM} parameter.
   */
  static Set<Integer> destinationBrokerIds(HttpServletRequest request) throws UnsupportedEncodingException {
    Set<Integer> brokerIds = Collections.unmodifiableSet(parseParamToIntegerSet(request, DESTINATION_BROKER_IDS_PARAM));
    if (!brokerIds.isEmpty()) {
      if (isKafkaAssignerMode(request)) {
        throw new UserRequestException("Kafka assigner mode does not support explicitly specifying destination broker ids.");
      }

      // Sanity check: Ensure that BROKER_ID_PARAM and DESTINATION_BROKER_IDS_PARAM configs share nothing.
      ensureDisjoint(parseParamToIntegerSet(request, BROKER_ID_PARAM), brokerIds,
                     "No overlap is allowed between the specified destination broker ids and broker ids");
    }

    return brokerIds;
  }

  /**
   * Default: An empty set.
   * @param request Http servlet request.
   * @param isApprove {@code true} for {@link #APPROVE_PARAM}, {@code false} for {@link #DISCARD_PARAM}
   * @return The value of {@link #APPROVE_PARAM} parameter for approve or {@link #DISCARD_PARAM} parameter for discard.
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
   * @param request Http servlet request.
   * @return {@link ReviewStatus#APPROVED} and {@link ReviewStatus#DISCARDED} requests via {@link #APPROVE_PARAM} and {@link #DISCARD_PARAM}.
   */
  static Map<ReviewStatus, Set<Integer>> reviewRequests(HttpServletRequest request) throws UnsupportedEncodingException {
    Set<Integer> approve = review(request, true);
    Set<Integer> discard = review(request, false);

    // Sanity check: Ensure that the same request is not specified in both configs at the same request.
    ensureDisjoint(approve, discard, "The same request cannot be specified in both approve and discard parameters");
    // Sanity check: Ensure that at least one approve or discard parameter is specified.
    if (approve.isEmpty() && discard.isEmpty()) {
      throw new UserRequestException(String.format("%s endpoint requires at least one of '%s' or '%s' parameter.",
                                                   REVIEW, APPROVE_PARAM, DISCARD_PARAM));
    }

    Map<ReviewStatus, Set<Integer>> reviewRequest = Map.of(APPROVED, approve, DISCARDED, discard);
    return reviewRequest;
  }

  /**
   * Default: An empty map.
   * @param request Http servlet request.
   * @return The value for {@link #BROKER_ID_AND_LOGDIRS_PARAM} parameter.
   */
  static Map<Integer, Set<String>> brokerIdAndLogdirs(HttpServletRequest request) throws UnsupportedEncodingException {
    final Map<Integer, Set<String>> brokerIdAndLogdirs = new HashMap<>();
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), BROKER_ID_AND_LOGDIRS_PARAM);
    if (parameterString != null) {
      Arrays.stream(urlDecode(request.getParameter(parameterString)).split(",")).forEach(e -> {
        int index = e.indexOf(DELIMITER_BETWEEN_BROKER_ID_AND_LOGDIR);
        int brokerId = Integer.parseInt(e.substring(0, index));
        brokerIdAndLogdirs.putIfAbsent(brokerId, new HashSet<>());
        brokerIdAndLogdirs.get(brokerId).add(e.substring(index + 1));
      });
    }
    return Collections.unmodifiableMap(brokerIdAndLogdirs);
  }

  /**
   * Default: An empty set.
   * @param request Http servlet request.
   * @return User task ids.
   */
  public static Set<UUID> userTaskIds(HttpServletRequest request) throws UnsupportedEncodingException {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), USER_TASK_IDS_PARAM);
    return parameterString == null
           ? Collections.emptySet()
           : Arrays.stream(urlDecode(request.getParameter(parameterString)).split(",")).map(UUID::fromString).collect(Collectors.toSet());
  }

  /**
   * Default: An empty set.
   * @param request Http servlet request.
   * @return Client ids.
   */
  public static Set<String> clientIds(HttpServletRequest request) throws UnsupportedEncodingException {
    Set<String> parsedClientIds = parseParamToStringSet(request, CLIENT_IDS_PARAM);
    // May need to validate clientIds
    return Collections.unmodifiableSet(parsedClientIds);
  }

  /**
   * Default: An empty set.
   * @param request Http servlet request.
   * @return Endpoints.
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
   * @param request Http servlet request.
   * @return Types.
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
   * @param request Http servlet request.
   * @return The value for {@link #DATA_FROM_PARAM} parameter.
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
   * Check whether to skip the hard goal check.
   * @param request Http servlet request.
   * @return {@code true} if hard goal check should be skipped, {@code false} otherwise.
   */
  static boolean skipHardGoalCheck(HttpServletRequest request) {
    return isKafkaAssignerMode(request) || isRebalanceDiskMode(request) || getBooleanParam(request, SKIP_HARD_GOAL_CHECK_PARAM, false);
  }

  static boolean skipUrpDemotion(HttpServletRequest request) {
    return getBooleanParam(request, SKIP_URP_DEMOTION_PARAM, true);
  }

  static boolean excludeFollowerDemotion(HttpServletRequest request) {
    return getBooleanParam(request, EXCLUDE_FOLLOWER_DEMOTION_PARAM, true);
  }

  static Short replicationFactor(HttpServletRequest request) {
    String parameterString = caseSensitiveParameterName(request.getParameterMap(), REPLICATION_FACTOR_PARAM);
    if (parameterString == null) {
      return null;
    }
    return Short.parseShort(request.getParameter(parameterString));
  }

  static boolean fetchCompletedTask(HttpServletRequest request) {
    return getBooleanParam(request, FETCH_COMPLETED_TASK_PARAM, false);
  }

  /**
   * Check whether all the passed-in parameters are {@code null} or not.
   *
   * @param parameters Arbitrary number of parameters to check.
   * @return {@code true} if all parameters are null; {@code false} otherwise.
   */
  public static boolean areAllParametersNull(CruiseControlParameters... parameters) {
    return Arrays.stream(parameters).allMatch(Objects::isNull);
  }

  public enum DataFrom {
    VALID_WINDOWS, VALID_PARTITIONS
  }
}
