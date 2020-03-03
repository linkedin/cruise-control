/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.CruiseControlParametersConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.CruiseControlRequestConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.UserTaskManagerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.linkedin.kafka.cruisecontrol.servlet.security.BasicSecurityProvider;
import com.linkedin.kafka.cruisecontrol.servlet.security.SecurityProvider;
import com.linkedin.kafka.cruisecontrol.servlet.security.jwt.JwtSecurityProvider;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;

/**
 * The configuration class of Kafka Cruise Control.
 *
 * To avoid having a huge monolithic class that mixes unrelated configs, config names, their defaults, and definitions
 * reside in the relevant classes under {@link com.linkedin.kafka.cruisecontrol.config.constants}.
 */
public class KafkaCruiseControlConfig extends AbstractConfig {
  private static final ConfigDef CONFIG;

  static {
    CONFIG = CruiseControlRequestConfig.define(CruiseControlParametersConfig.define(AnomalyDetectorConfig.define(
        AnalyzerConfig.define(ExecutorConfig.define(MonitorConfig.define(WebServerConfig.define(
            UserTaskManagerConfig.define(new ConfigDef())))))))).withClientSslSupport().withClientSaslSupport();
  }

  /**
   * @return Merged config values.
   */
  public Map<String, Object> mergedConfigValues() {
    Map<String, Object> conf = originals();

    // Use parsed non-null value to overwrite originals.
    // This will keep default values and also keep values that are not defined under ConfigDef.
    values().forEach((k, v) -> {
      if (v != null) {
        conf.put(k, v);
      }
    });
    return conf;
  }

  @Override
  public <T> T getConfiguredInstance(String key, Class<T> t) {
    T o = super.getConfiguredInstance(key, t);
    if (o instanceof CruiseControlConfigurable) {
      ((CruiseControlConfigurable) o).configure(mergedConfigValues());
    }
    return o;
  }

  @Override
  public <T> List<T> getConfiguredInstances(String key, Class<T> t) {
    List<T> objects = super.getConfiguredInstances(key, t);
    for (T o : objects) {
      if (o instanceof CruiseControlConfigurable) {
        ((CruiseControlConfigurable) o).configure(mergedConfigValues());
      }
    }
    return objects;
  }

  @Override
  public <T> List<T> getConfiguredInstances(String key, Class<T> t, Map<String, Object> configOverrides) {
    List<T> objects = super.getConfiguredInstances(key, t, configOverrides);
    Map<String, Object> configPairs = mergedConfigValues();
    configPairs.putAll(configOverrides);
    for (T o : objects) {
      if (o instanceof CruiseControlConfigurable) {
        ((CruiseControlConfigurable) o).configure(configPairs);
      }
    }
    return objects;
  }

  /**
   * @return Configured instance.
   */
  public <T> T getConfiguredInstance(String key, Class<T> t, Map<String, Object> configOverrides) {
    Class<?> c = getClass(key);
    Map<String, Object> configPairs = mergedConfigValues();
    configPairs.putAll(configOverrides);
    return KafkaCruiseControlConfigUtils.getConfiguredInstance(c, t, configPairs);
  }

  /**
   * Sanity check for
   * <ul>
   *   <li>{@link AnalyzerConfig#GOALS_CONFIG} and {@link AnalyzerConfig#INTRA_BROKER_GOALS_CONFIG} are non-empty.</li>
   *   <li>Case insensitive goal names.</li>
   *   <li>{@link AnalyzerConfig#DEFAULT_GOALS_CONFIG} is non-empty.</li>
   *   <li>{@link AnomalyDetectorConfig#SELF_HEALING_GOALS_CONFIG} is a sublist of {@link AnalyzerConfig#GOALS_CONFIG}.</li>
   *   <li>{@link AnomalyDetectorConfig#ANOMALY_DETECTION_GOALS_CONFIG} is a sublist of
   *   (1) {@link AnomalyDetectorConfig#SELF_HEALING_GOALS_CONFIG} if it is not empty,
   *   (2) {@link AnalyzerConfig#DEFAULT_GOALS_CONFIG} otherwise.</li>
   * </ul>
   */
  private void sanityCheckGoalNames() {
    List<String> goalNames = getList(AnalyzerConfig.GOALS_CONFIG);
    // Ensure that goals are non-empty.
    if (goalNames.isEmpty()) {
      throw new ConfigException("Attempt to configure goals configuration with an empty list of goals.");
    }

    List<String> intraBrokerGoalNames = getList(AnalyzerConfig.INTRA_BROKER_GOALS_CONFIG);
    // Ensure that intra-broker goals are non-empty.
    if (intraBrokerGoalNames.isEmpty()) {
      throw new ConfigException("Attempt to configure intra-broker goals configuration with an empty list of goals.");
    }

    Set<String> caseInsensitiveGoalNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    for (String goalName: goalNames) {
      if (!caseInsensitiveGoalNames.add(goalName.replaceAll(".*\\.", ""))) {
        throw new ConfigException("Attempt to configure goals with case sensitive names.");
      }
    }
    for (String goalName: intraBrokerGoalNames) {
      if (!caseInsensitiveGoalNames.add(goalName.replaceAll(".*\\.", ""))) {
        throw new ConfigException("Attempt to configure intra-broker goals with case sensitive names.");
      }
    }

    // Ensure that default goals is non-empty.
    List<String> defaultGoalNames = getList(AnalyzerConfig.DEFAULT_GOALS_CONFIG);
    if (defaultGoalNames.isEmpty()) {
      throw new ConfigException("Attempt to configure default goals configuration with an empty list of goals.");
    }

    // Ensure that goals used for self-healing are supported goals.
    List<String> selfHealingGoalNames = getList(AnomalyDetectorConfig.SELF_HEALING_GOALS_CONFIG);
    if (selfHealingGoalNames.stream().anyMatch(g -> !defaultGoalNames.contains(g))) {
      throw new ConfigException(String.format("Attempt to configure self healing goals with unsupported goals (%s:%s and %s:%s).",
                                              AnomalyDetectorConfig.SELF_HEALING_GOALS_CONFIG, selfHealingGoalNames,
                                              AnalyzerConfig.DEFAULT_GOALS_CONFIG, defaultGoalNames));
    }

    // Ensure that goals used for anomaly detection are a subset of goals used for fixing the anomaly.
    List<String> anomalyDetectionGoalNames = getList(AnomalyDetectorConfig.ANOMALY_DETECTION_GOALS_CONFIG);
    if (anomalyDetectionGoalNames.stream().anyMatch(g -> selfHealingGoalNames.isEmpty() ? !defaultGoalNames.contains(g)
                                                                                        : !selfHealingGoalNames.contains(g))) {
      throw new ConfigException("Attempt to configure anomaly detection goals as a superset of self healing goals.");
    }
  }

  /**
   * Sanity check to ensure that
   * <ul>
   *   <li>{@link ExecutorConfig#MAX_NUM_CLUSTER_MOVEMENTS_CONFIG} >
   *     {@link ExecutorConfig#NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG}</li>
   *   <li>{@link ExecutorConfig#MAX_NUM_CLUSTER_MOVEMENTS_CONFIG} >
   *     {@link ExecutorConfig#NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_CONFIG}</li>
   *   <li>{@link ExecutorConfig#MAX_NUM_CLUSTER_MOVEMENTS_CONFIG} >=
   *   {@link ExecutorConfig#NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG}</li>
   * </ul>
   */
  private void sanityCheckConcurrency() {
    int maxClusterPartitionMovementConcurrency = getInt(ExecutorConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG);

    int interBrokerPartitionMovementConcurrency = getInt(ExecutorConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG);
    if (interBrokerPartitionMovementConcurrency >= maxClusterPartitionMovementConcurrency) {
      throw new ConfigException("Inter-broker partition movement concurrency [" + interBrokerPartitionMovementConcurrency
                                + "] must be smaller than the maximum number of allowed movements in cluster ["
                                + maxClusterPartitionMovementConcurrency + "].");
    }

    int intraBrokerPartitionMovementConcurrency = getInt(ExecutorConfig.NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_CONFIG);
    if (intraBrokerPartitionMovementConcurrency >= maxClusterPartitionMovementConcurrency) {
      throw new ConfigException("Intra-broker partition movement concurrency [" + intraBrokerPartitionMovementConcurrency
                                + "] must be smaller than the maximum number of allowed movements in cluster ["
                                + maxClusterPartitionMovementConcurrency + "].");
    }

    int leadershipMovementConcurrency = getInt(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG);
    if (leadershipMovementConcurrency > maxClusterPartitionMovementConcurrency) {
      throw new ConfigException("Leadership movement concurrency [" + leadershipMovementConcurrency
                                + "] cannot be greater than the maximum number of allowed movements in cluster ["
                                + maxClusterPartitionMovementConcurrency + "].");
    }
  }

  /**
   * Sanity check to ensure that {@link ExecutorConfig#LEADER_MOVEMENT_TIMEOUT_MS_CONFIG} >
   * {@link ExecutorConfig#TASK_EXECUTION_ALERTING_THRESHOLD_MS_CONFIG}
   */
  private void sanityCheckTaskExecutionAlertingThreshold() {
    long leaderMovementTimeoutMs = getLong(ExecutorConfig.LEADER_MOVEMENT_TIMEOUT_MS_CONFIG);
    long taskExecutionAlertingThresholdMs = getLong(ExecutorConfig.TASK_EXECUTION_ALERTING_THRESHOLD_MS_CONFIG);
    if (taskExecutionAlertingThresholdMs >= leaderMovementTimeoutMs) {
      throw new ConfigException("Task execution time alerting threshold [" + taskExecutionAlertingThresholdMs
                                + "ms] cannot be greater than leader movement timeout [" + leaderMovementTimeoutMs + "ms].");
    }
  }

  /**
   * Sanity check to ensure that
   * <ul>
   *   <li>{@link MonitorConfig#METADATA_MAX_AGE_CONFIG} is not longer than {@link MonitorConfig#METRIC_SAMPLING_INTERVAL_MS_CONFIG},</li>
   *   <li>sampling frequency per partition window is within the limits -- i.e. ({@link MonitorConfig#PARTITION_METRICS_WINDOW_MS_CONFIG} /
   *   {@link MonitorConfig#METRIC_SAMPLING_INTERVAL_MS_CONFIG}) <= {@link Byte#MAX_VALUE}, and</li>
   *   <li>sampling frequency per broker window is within the limits -- i.e. ({@link MonitorConfig#BROKER_METRICS_WINDOW_MS_CONFIG} /
   *   {@link MonitorConfig#METRIC_SAMPLING_INTERVAL_MS_CONFIG}) <= {@link Byte#MAX_VALUE}, and</li>
   *   <li>{@link CruiseControlMetricsReporterConfig#CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_CONFIG} is not longer than
   *   {@link MonitorConfig#METRIC_SAMPLING_INTERVAL_MS_CONFIG}, and</li>
   *   <li>{@link MonitorConfig#METRIC_SAMPLING_INTERVAL_MS_CONFIG} is not longer than
   *   {@link AnomalyDetectorConfig#METRIC_ANOMALY_DETECTION_INTERVAL_MS_CONFIG}
   *   (or #ANOMALY_DETECTION_INTERVAL_MS_CONFIG if #METRIC_ANOMALY_DETECTION_INTERVAL_MS_CONFIG is not specified)</li>
   * </ul>
   *
   * Sampling process involves a potential metadata update if the current metadata is stale. The configuration
   * {@link MonitorConfig#METADATA_MAX_AGE_CONFIG} indicates the timeout of such a metadata update. Hence, this subprocess of the sampling
   * process cannot be set with a timeout larger than the total sampling timeout of {@link MonitorConfig#METRIC_SAMPLING_INTERVAL_MS_CONFIG}.
   *
   * The number of samples at a given window cannot exceed a predefined maximum limit.
   *
   * The metrics reporting interval should not be larger than the metrics sampling interval in order to ensure there is always
   * data to be collected.
   */
  private void sanityCheckSamplingPeriod(Map<?, ?> originals) {
    long samplingIntervalMs = getLong(MonitorConfig.METRIC_SAMPLING_INTERVAL_MS_CONFIG);
    long metadataTimeoutMs = getLong(MonitorConfig.METADATA_MAX_AGE_CONFIG);
    if (metadataTimeoutMs > samplingIntervalMs) {
      throw new ConfigException("Attempt to set metadata refresh timeout [" + metadataTimeoutMs +
                                "] to be longer than sampling period [" + samplingIntervalMs + "].");
    }

    // Ensure that the sampling frequency per partition window is within the limits.
    long partitionSampleWindowMs = getLong(MonitorConfig.PARTITION_METRICS_WINDOW_MS_CONFIG);
    short partitionSamplingFrequency = (short) (partitionSampleWindowMs / samplingIntervalMs);
    if (partitionSamplingFrequency > Byte.MAX_VALUE) {
      throw new ConfigException(String.format("Configured sampling frequency (%d) exceeds the maximum allowed value (%d). "
                                              + "Decrease the value of %s or increase the value of %s to ensure that their"
                                              + " ratio is under this limit.", partitionSamplingFrequency, Byte.MAX_VALUE,
                                              MonitorConfig.PARTITION_METRICS_WINDOW_MS_CONFIG,
                                              MonitorConfig.METRIC_SAMPLING_INTERVAL_MS_CONFIG));
    }

    // Ensure that the sampling frequency per broker window is within the limits.
    long brokerSampleWindowMs = getLong(MonitorConfig.BROKER_METRICS_WINDOW_MS_CONFIG);
    short brokerSamplingFrequency = (short) (brokerSampleWindowMs / samplingIntervalMs);
    if (brokerSamplingFrequency > Byte.MAX_VALUE) {
      throw new ConfigException(String.format("Configured sampling frequency (%d) exceeds the maximum allowed value (%d). "
                                              + "Decrease the value of %s or increase the value of %s to ensure that their"
                                              + " ratio is under this limit.", brokerSamplingFrequency, Byte.MAX_VALUE,
                                              MonitorConfig.BROKER_METRICS_WINDOW_MS_CONFIG,
                                              MonitorConfig.METRIC_SAMPLING_INTERVAL_MS_CONFIG));
    }

    // Ensure that the metrics reporter reports more often that the sample fetcher samples.
    CruiseControlMetricsReporterConfig reporterConfig = new CruiseControlMetricsReporterConfig(originals, false);
    long reportingIntervalMs = reporterConfig.getLong(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_CONFIG);
    if (reportingIntervalMs > samplingIntervalMs) {
      throw new ConfigException(String.format("Configured metric reporting interval (%d) exceeds metric sampling interval (%d). "
                                              + "Decrease the value of %s or increase the value of %s to ensure that reported "
                                              + "metrics can be properly sampled.",
                                              reportingIntervalMs, samplingIntervalMs,
                                              CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_CONFIG,
                                              MonitorConfig.METRIC_SAMPLING_INTERVAL_MS_CONFIG));
    }

    // Ensure sampling frequency is is higher than metric anomaly detection frequency.
    Long metricAnomalyDetectionIntervalMs = getLong(AnomalyDetectorConfig.METRIC_ANOMALY_DETECTION_INTERVAL_MS_CONFIG);
    if (metricAnomalyDetectionIntervalMs == null) {
      metricAnomalyDetectionIntervalMs = getLong(AnomalyDetectorConfig.ANOMALY_DETECTION_INTERVAL_MS_CONFIG);
    }
    if (samplingIntervalMs > metricAnomalyDetectionIntervalMs) {
      throw new ConfigException(String.format("Configured metric sampling interval (%d) exceeds metric anomaly detection interval (%d). "
                                              + "Decrease the value of %s or increase the value of %s (or %s if %s is not specified) to "
                                              + "ensure that metrics anomaly detection does not run too frequently.",
                                              samplingIntervalMs, metricAnomalyDetectionIntervalMs,
                                              MonitorConfig.METRIC_SAMPLING_INTERVAL_MS_CONFIG,
                                              AnomalyDetectorConfig.METRIC_ANOMALY_DETECTION_INTERVAL_MS_CONFIG,
                                              AnomalyDetectorConfig.ANOMALY_DETECTION_INTERVAL_MS_CONFIG,
                                              AnomalyDetectorConfig.METRIC_ANOMALY_DETECTION_INTERVAL_MS_CONFIG));
    }
  }

  /**
   * Sanity check to ensure that SSL and authentication is set up correctly. This means the following:
   * <ul>
   *   <li>If <code>webserver.ssl.enable</code> is set, then <code>webserver.ssl.keystore.location</code> and
   *   <code>webserver.ssl.protocol</code> must also be set.</li>
   *   <li><code>If webserver.security.enable</code> is set, then <code>webserver.security.provider</code> must also be
   *   set to be an implementation of the {@link SecurityProvider} interface. Also if it's {@link BasicSecurityProvider}
   *   then <code>basic.auth.credentials.file</code> must point to an existing file.</li>
   * </ul>
   */
  void sanityCheckSecurity() { // visible for testing
    Boolean sslEnabled = getBoolean(WebServerConfig.WEBSERVER_SSL_ENABLE_CONFIG);
    if (sslEnabled) {
      if (getString(WebServerConfig.WEBSERVER_SSL_KEYSTORE_LOCATION_CONFIG) == null) {
        throw new ConfigException("If webserver SSL is enabled, the keystore location must be set.");
      }
      if (getString(WebServerConfig.WEBSERVER_SSL_PROTOCOL_CONFIG) == null) {
        throw new ConfigException("If webserver SSL is enabled, a valid SSL protocol must be set.");
      }
    }
    Boolean securityEnabled = getBoolean(WebServerConfig.WEBSERVER_SECURITY_ENABLE_CONFIG);
    if (securityEnabled) {
      Class<?> securityProvider = getClass(WebServerConfig.WEBSERVER_SECURITY_PROVIDER_CONFIG);
      if (securityProvider == null || !SecurityProvider.class.isAssignableFrom(securityProvider)) {
        throw new ConfigException(String.format("If webserver security is enabled, a valid security provider must be set " +
            "that is an implementation of %s.", SecurityProvider.class.getName()));
      }
      String authCredentialsFile = getString(WebServerConfig.WEBSERVER_AUTH_CREDENTIALS_FILE_CONFIG);
      if (BasicSecurityProvider.class == securityProvider && !fileExists(authCredentialsFile)) {
        throw new ConfigException(String.format("If %s is used, an existing credentials file must be set.",
            BasicSecurityProvider.class.getName()));
      }
      if (JwtSecurityProvider.class == securityProvider) {
        String providerUrl = getString(WebServerConfig.JWT_AUTHENTICATION_PROVIDER_URL_CONFIG);
        if (providerUrl == null || providerUrl.isEmpty()) {
          throw new ConfigException(String.format("When %s is used, %s must be set.",
              JwtSecurityProvider.class.getName(), WebServerConfig.JWT_AUTHENTICATION_PROVIDER_URL_CONFIG));
        }
        String certificateFile = getString(WebServerConfig.JWT_AUTH_CERTIFICATE_LOCATION_CONFIG);
        if (!fileExists(certificateFile)) {
          throw new ConfigException(String.format("If %s is used, an existing certificate file must be set.",
              JwtSecurityProvider.class.getName()));
        }
        String privilegesFile = getString(WebServerConfig.WEBSERVER_AUTH_CREDENTIALS_FILE_CONFIG);
        if (!fileExists(privilegesFile)) {
          throw new ConfigException(String.format("If %s is used, an existing certificate file must be set.",
              JwtSecurityProvider.class.getName()));
        }
      }
    }
  }

  private boolean fileExists(String file) {
    return file != null && Files.exists(Paths.get(file));
  }

  public KafkaCruiseControlConfig(Map<?, ?> originals) {
    this(originals, true);
  }

  public KafkaCruiseControlConfig(Map<?, ?> originals, boolean doLog) {
    super(CONFIG, originals, doLog);
    sanityCheckGoalNames();
    sanityCheckSamplingPeriod(originals);
    sanityCheckConcurrency();
    sanityCheckTaskExecutionAlertingThreshold();
    sanityCheckSecurity();
  }
}
