/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config.constants;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal;
import com.linkedin.kafka.cruisecontrol.detector.BrokerFailures;
import com.linkedin.kafka.cruisecontrol.detector.DiskFailures;
import com.linkedin.kafka.cruisecontrol.detector.GoalViolations;
import com.linkedin.kafka.cruisecontrol.detector.KafkaMetricAnomaly;
import com.linkedin.kafka.cruisecontrol.detector.NoopMetricAnomalyFinder;
import com.linkedin.kafka.cruisecontrol.detector.notifier.NoopNotifier;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.Range.between;


/**
 * A class to keep Cruise Control Anomaly Detector Configs and defaults.
 * DO NOT CHANGE EXISTING CONFIG NAMES AS CHANGES WOULD BREAK USER CODE.
 */
public class AnomalyDetectorConfig {
  public static final boolean DEFAULT_SELF_HEALING_EXCLUDE_RECENT_BROKERS_CONFIG = true;

  private AnomalyDetectorConfig() {

  }

  /**
   * <code>metric.anomaly.finder.class</code>
   */
  public static final String METRIC_ANOMALY_FINDER_CLASSES_CONFIG = "metric.anomaly.finder.class";
  public static final String DEFAULT_METRIC_ANOMALY_FINDER_CLASS = NoopMetricAnomalyFinder.class.getName();
  public static final String METRIC_ANOMALY_FINDER_CLASSES_DOC = "A list of metric anomaly finder classes to find the current "
                                                          + "state to identify metric anomalies.";

  /**
   * <code>num.cached.recent.anomaly.states</code>
   */
  public static final String NUM_CACHED_RECENT_ANOMALY_STATES_CONFIG = "num.cached.recent.anomaly.states";
  public static final String NUM_CACHED_RECENT_ANOMALY_STATES_DOC = "The number of recent anomaly states cached for "
      + "different anomaly types presented via the anomaly substate response of the state endpoint.";

  /**
   * <code>broker.failures.class</code>
   */
  public static final String BROKER_FAILURES_CLASS_CONFIG = "broker.failures.class";
  public static final String DEFAULT_BROKER_FAILURES_CLASS = BrokerFailures.class.getName();
  public static final String BROKER_FAILURES_CLASS_DOC = String.format("The %s class that extends broker failures.",
                                                                DEFAULT_BROKER_FAILURES_CLASS);

  /**
   * <code>goal.violations.class</code>
   */
  public static final String GOAL_VIOLATIONS_CLASS_CONFIG = "goal.violations.class";
  public static final String DEFAULT_GOAL_VIOLATIONS_CLASS = GoalViolations.class.getName();
  public static final String GOAL_VIOLATIONS_CLASS_DOC = String.format("The %s class that extends goal violations.",
                                                                DEFAULT_GOAL_VIOLATIONS_CLASS);

  /**
   * <code>disk.failures.class</code>
   */
  public static final String DISK_FAILURES_CLASS_CONFIG = "disk.failures.class";
  public static final String DEFAULT_DISK_FAILURES_CLASS = DiskFailures.class.getName();
  public static final String DISK_FAILURES_CLASS_DOC = String.format("The %s class that extends disk failures.",
                                                              DEFAULT_DISK_FAILURES_CLASS);

  /**
   * <code>metric.anomaly.class</code>
   */
  public static final String METRIC_ANOMALY_CLASS_CONFIG = "metric.anomaly.class";
  public static final String DEFAULT_METRIC_ANOMALY_CLASS = KafkaMetricAnomaly.class.getName();
  public static final String METRIC_ANOMALY_CLASS_DOC = String.format("The %s class that extends metric anomaly.",
                                                               DEFAULT_METRIC_ANOMALY_CLASS);

  /**
   * <code>self.healing.goals</code>
   */
  public static final String SELF_HEALING_GOALS_CONFIG = "self.healing.goals";
  public static final List DEFAULT_SELF_HEALING_GOALS = Collections.emptyList();
  public static final String SELF_HEALING_GOALS_DOC = "The list of goals to be used for self-healing relevant anomalies."
      + " If empty, uses the default.goals for self healing.";

  /**
   * <code>anomaly.notifier.class</code>
   */
  public static final String ANOMALY_NOTIFIER_CLASS_CONFIG = "anomaly.notifier.class";
  public static final String DEFAULT_ANOMALY_NOTIFIER_CLASS = NoopNotifier.class.getName();
  public static final String ANOMALY_NOTIFIER_CLASS_DOC = "The notifier class to trigger an alert when an "
      + "anomaly is violated. The anomaly could be either a goal violation, broker failure, or metric anomaly.";

  /**
   * <code>anomaly.detection.goals</code>
   */
  public static final String ANOMALY_DETECTION_GOALS_CONFIG = "anomaly.detection.goals";
  public static final String DEFAULT_ANOMALY_DETECTION_GOALS = new StringJoiner(",").add(RackAwareGoal.class.getName())
                                                                                    .add(ReplicaCapacityGoal.class.getName())
                                                                                    .add(DiskCapacityGoal.class.getName()).toString();
  public static final String ANOMALY_DETECTION_GOALS_DOC = "The goals that anomaly detector should detect if they are violated.";

  /**
   * <code>self.healing.exclude.recently.demoted.brokers</code>
   */
  public static final String SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG = "self.healing.exclude.recently.demoted.brokers";
  public static final String SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_DOC = "True if recently demoted brokers are excluded "
      + "from optimizations during self healing, false otherwise.";

  /**
   * <code>self.healing.exclude.recently.removed.brokers</code>
   */
  public static final String SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG = "self.healing.exclude.recently.removed.brokers";
  public static final String SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_DOC = "True if recently removed brokers "
      + "are excluded from optimizations during self healing, false otherwise.";

  /**
   * <code>failed.brokers.zk.path</code>
   */
  public static final String FAILED_BROKERS_ZK_PATH_CONFIG = "failed.brokers.zk.path";
  public static final String DEFAULT_FAILED_BROKERS_ZK_PATH = "/CruiseControlBrokerList";
  public static final String FAILED_BROKERS_ZK_PATH_DOC = "The zk path to store the failed broker list. This is to "
      + "persist the broker failure time in case Cruise Control failed and restarted when some brokers are down.";

  /**
   * <code>anomaly.detection.interval.ms</code>
   */
  public static final String ANOMALY_DETECTION_INTERVAL_MS_CONFIG = "anomaly.detection.interval.ms";
  public static final String ANOMALY_DETECTION_INTERVAL_MS_DOC = "The interval in millisecond that the detectors will "
      + "run to detect the anomalies.";

  /**
   * <code>goal.violation.detection.interval.ms</code>
   */
  public static final String GOAL_VIOLATION_DETECTION_INTERVAL_MS_CONFIG = "goal.violation.detection.interval.ms";
  public static final String GOAL_VIOLATION_DETECTION_INTERVAL_MS_DOC = "The interval in millisecond that goal violation "
      + "detector will run to detect goal violations. If this interval time is not specified, goal violation detector "
      + "will run with interval specified in " + ANOMALY_DETECTION_INTERVAL_MS_CONFIG + ".";

  /**
   * <code>metric.anomaly.detection.interval.ms</code>
   */
  public static final String METRIC_ANOMALY_DETECTION_INTERVAL_MS_CONFIG = "metric.anomaly.detection.interval.ms";
  public static final String METRIC_ANOMALY_DETECTION_INTERVAL_MS_DOC = "The interval in millisecond that metric anomaly "
      + "detector will run to detect metric anomalies. If this interval time is not specified, metric anomaly detector "
      + "will run with interval specified in " + ANOMALY_DETECTION_INTERVAL_MS_CONFIG + ".";

  /**
   * <code>disk.failure.detection.interval.ms</code>
   */
  public static final String DISK_FAILURE_DETECTION_INTERVAL_MS_CONFIG = "disk.failure.detection.interval.ms";
  public static final String DISK_FAILURE_DETECTION_INTERVAL_MS_DOC = "The interval in millisecond that disk failure "
      + "detector will run to detect disk failures. If this interval time is not specified, disk failure detector "
      + "will run with interval specified in " + ANOMALY_DETECTION_INTERVAL_MS_CONFIG + ".";

  /**
   * <code>broker.failure.detection.backoff.ms</code>
   */
  public static final String BROKER_FAILURE_DETECTION_BACKOFF_MS_CONFIG = "broker.failure.detection.backoff.ms";
  public static final String BROKER_FAILURE_DETECTION_BACKOFF_MS_DOC = "The backoff time in millisecond before broker failure "
      + "detector triggers another broker failure detection if currently detected broker failure is not ready to fix.";

  /**
   * <code>anomaly.detection.allow.capacity.estimation</code>
   */
  public static final String ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG = "anomaly.detection.allow.capacity.estimation";
  public static final boolean DEFAULT_ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG = true;
  public static final String ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_DOC = "The flag to indicate whether anomaly "
      + "detection threads allow capacity estimation in the generated cluster model they use.";

  /**
   * Define configs for Anomaly Detector.
   *
   * @param configDef Config definition.
   * @return The given ConfigDef after defining the configs for Anomaly Detector.
   */
  public static ConfigDef define(ConfigDef configDef) {
    return configDef.define(METRIC_ANOMALY_FINDER_CLASSES_CONFIG,
                            ConfigDef.Type.LIST,
                            DEFAULT_METRIC_ANOMALY_FINDER_CLASS,
                            ConfigDef.Importance.MEDIUM,
                            METRIC_ANOMALY_FINDER_CLASSES_DOC)
                    .define(NUM_CACHED_RECENT_ANOMALY_STATES_CONFIG,
                            ConfigDef.Type.INT,
                            10,
                            between(1, 100),
                            ConfigDef.Importance.LOW,
                            NUM_CACHED_RECENT_ANOMALY_STATES_DOC)
                    .define(BROKER_FAILURES_CLASS_CONFIG,
                            ConfigDef.Type.CLASS,
                            DEFAULT_BROKER_FAILURES_CLASS,
                            ConfigDef.Importance.MEDIUM,
                            BROKER_FAILURES_CLASS_DOC)
                    .define(GOAL_VIOLATIONS_CLASS_CONFIG,
                            ConfigDef.Type.CLASS,
                            DEFAULT_GOAL_VIOLATIONS_CLASS,
                            ConfigDef.Importance.MEDIUM,
                            GOAL_VIOLATIONS_CLASS_DOC)
                    .define(DISK_FAILURES_CLASS_CONFIG,
                            ConfigDef.Type.CLASS,
                            DEFAULT_DISK_FAILURES_CLASS,
                            ConfigDef.Importance.MEDIUM,
                            DISK_FAILURES_CLASS_DOC)
                    .define(METRIC_ANOMALY_CLASS_CONFIG,
                            ConfigDef.Type.CLASS,
                            DEFAULT_METRIC_ANOMALY_CLASS,
                            ConfigDef.Importance.MEDIUM,
                            METRIC_ANOMALY_CLASS_DOC)
                    .define(SELF_HEALING_GOALS_CONFIG,
                            ConfigDef.Type.LIST,
                            DEFAULT_SELF_HEALING_GOALS,
                            ConfigDef.Importance.HIGH,
                            SELF_HEALING_GOALS_DOC)
                    .define(ANOMALY_NOTIFIER_CLASS_CONFIG,
                            ConfigDef.Type.CLASS,
                            DEFAULT_ANOMALY_NOTIFIER_CLASS,
                            ConfigDef.Importance.LOW,
                            ANOMALY_NOTIFIER_CLASS_DOC)
                    .define(ANOMALY_DETECTION_GOALS_CONFIG,
                            ConfigDef.Type.LIST,
                            DEFAULT_ANOMALY_DETECTION_GOALS,
                            ConfigDef.Importance.MEDIUM,
                            ANOMALY_DETECTION_GOALS_DOC)
                    .define(SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG,
                            ConfigDef.Type.BOOLEAN,
                            DEFAULT_SELF_HEALING_EXCLUDE_RECENT_BROKERS_CONFIG,
                            ConfigDef.Importance.MEDIUM,
                            SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_DOC)
                    .define(SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG,
                            ConfigDef.Type.BOOLEAN,
                            DEFAULT_SELF_HEALING_EXCLUDE_RECENT_BROKERS_CONFIG,
                            ConfigDef.Importance.MEDIUM,
                            SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_DOC)
                    .define(FAILED_BROKERS_ZK_PATH_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_FAILED_BROKERS_ZK_PATH,
                            ConfigDef.Importance.LOW,
                            FAILED_BROKERS_ZK_PATH_DOC)
                    .define(ANOMALY_DETECTION_INTERVAL_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            300000L,
                            ConfigDef.Importance.LOW,
                            ANOMALY_DETECTION_INTERVAL_MS_DOC)
                    .define(GOAL_VIOLATION_DETECTION_INTERVAL_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            null,
                            ConfigDef.Importance.LOW,
                            GOAL_VIOLATION_DETECTION_INTERVAL_MS_DOC)
                    .define(METRIC_ANOMALY_DETECTION_INTERVAL_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            null,
                            ConfigDef.Importance.LOW,
                            METRIC_ANOMALY_DETECTION_INTERVAL_MS_DOC)
                    .define(DISK_FAILURE_DETECTION_INTERVAL_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            null,
                            ConfigDef.Importance.LOW,
                            DISK_FAILURE_DETECTION_INTERVAL_MS_DOC)
                    .define(BROKER_FAILURE_DETECTION_BACKOFF_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            300000L,
                            ConfigDef.Importance.LOW,
                            BROKER_FAILURE_DETECTION_BACKOFF_MS_DOC)
                    .define(ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG,
                            ConfigDef.Type.BOOLEAN,
                            DEFAULT_ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG,
                            ConfigDef.Importance.LOW,
                            ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_DOC);
  }
}
