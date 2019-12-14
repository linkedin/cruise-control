/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException;
import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricSampleCompleteness;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.common.MetadataClient;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigResolver;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.async.progress.GeneratingClusterModel;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.async.progress.WaitingForClusterModel;
import com.linkedin.kafka.cruisecontrol.config.TopicConfigProvider;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerEntity;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.PartitionEntity;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.KafkaBrokerMetricSampleAggregator;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.KafkaPartitionMetricSampleAggregator;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricSampleAggregationResult;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.PartitionMetricSample;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.SampleExtrapolation;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import com.linkedin.kafka.cruisecontrol.servlet.response.stats.BrokerStats;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig.SKIP_LOADING_SAMPLES_CONFIG;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils.getRackHandleNull;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils.getReplicaPlacementInfo;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils.partitionSampleExtrapolations;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils.populatePartitionLoad;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils.setBadBrokerState;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DEFAULT_START_TIME_FOR_CLUSTER_MODEL;

/**
 * The LoadMonitor monitors the workload of a Kafka cluster. It periodically triggers the metric sampling and
 * maintains the collected {@link PartitionMetricSample}. It is also responsible for aggregate the metrics samples into
 * {@link AggregatedMetricValues} for the analyzer to generate the balancing proposals.
 */
public class LoadMonitor {

  // Kafka Load Monitor server log.
  private static final Logger LOG = LoggerFactory.getLogger(LoadMonitor.class);
  // Metadata TTL is set based on experience -- i.e. a short TTL with large metadata may cause excessive load on brokers.
  private static final long METADATA_TTL = 10000L;
  private static final long METADATA_REFRESH_BACKOFF = 5000L;
  private final int _numPartitionMetricSampleWindows;
  private final LoadMonitorTaskRunner _loadMonitorTaskRunner;
  private final KafkaPartitionMetricSampleAggregator _partitionMetricSampleAggregator;
  private final KafkaBrokerMetricSampleAggregator _brokerMetricSampleAggregator;
  // A semaphore to help throttle the simultaneous cluster model creation
  private final Semaphore _clusterModelSemaphore;
  private final KafkaCruiseControlConfig _config;
  private final MetadataClient _metadataClient;
  private final AdminClient _adminClient;
  private final BrokerCapacityConfigResolver _brokerCapacityConfigResolver;
  private final TopicConfigProvider _topicConfigProvider;
  private final ScheduledExecutorService _loadMonitorExecutor;
  private final Timer _clusterModelCreationTimer;
  private final ThreadLocal<Boolean> _acquiredClusterModelSemaphore;
  private final ModelCompletenessRequirements _defaultModelCompletenessRequirements;

  // Sensor values
  private volatile int _numValidSnapshotWindows;
  private volatile double _monitoredPartitionsPercentage;
  private volatile int _totalMonitoredSnapshotWindows;
  private volatile int _numPartitionsWithExtrapolations;
  private volatile long _lastUpdate;

  private volatile ModelGeneration _cachedBrokerLoadGeneration;
  private volatile BrokerStats _cachedBrokerLoadStats;

  /**
   * Construct a load monitor.
   *
   * @param config The load monitor configuration.
   * @param time   The time object.
   * @param executor The proposal executor.
   * @param dropwizardMetricRegistry The sensor registry for cruise control
   * @param metricDef The metric definitions.
   */
  public LoadMonitor(KafkaCruiseControlConfig config,
                     Time time,
                     Executor executor,
                     MetricRegistry dropwizardMetricRegistry,
                     MetricDef metricDef) {
    this(config,
         new MetadataClient(config,
                            new Metadata(METADATA_REFRESH_BACKOFF,
                                         config.getLong(MonitorConfig.METADATA_MAX_AGE_CONFIG),
                                         new LogContext(),
                                         new ClusterResourceListeners()),
                            METADATA_TTL,
                            time),
         KafkaCruiseControlUtils.createAdminClient(KafkaCruiseControlUtils.parseAdminClientConfigs(config)),
         time,
         executor,
         dropwizardMetricRegistry,
         metricDef);
  }

  /**
   * Package private constructor for unit tests.
   */
  LoadMonitor(KafkaCruiseControlConfig config,
              MetadataClient metadataClient,
              AdminClient adminClient,
              Time time,
              Executor executor,
              MetricRegistry dropwizardMetricRegistry,
              MetricDef metricDef) {
    _config = config;

    _metadataClient = metadataClient;

    _adminClient = adminClient;

    _brokerCapacityConfigResolver = config.getConfiguredInstance(MonitorConfig.BROKER_CAPACITY_CONFIG_RESOLVER_CLASS_CONFIG,
                                                                 BrokerCapacityConfigResolver.class);
    _topicConfigProvider = config.getConfiguredInstance(MonitorConfig.TOPIC_CONFIG_PROVIDER_CLASS_CONFIG,
                                                                 TopicConfigProvider.class);
    _numPartitionMetricSampleWindows = config.getInt(MonitorConfig.NUM_PARTITION_METRICS_WINDOWS_CONFIG);

    _partitionMetricSampleAggregator = new KafkaPartitionMetricSampleAggregator(config, metadataClient.metadata());

    _brokerMetricSampleAggregator = new KafkaBrokerMetricSampleAggregator(config);

    _acquiredClusterModelSemaphore = ThreadLocal.withInitial(() -> false);

    // We use the number of proposal precomputing threads config to ensure there is enough concurrency if users
    // wants that.
    int numPrecomputingThread = config.getInt(AnalyzerConfig.NUM_PROPOSAL_PRECOMPUTE_THREADS_CONFIG);
    _clusterModelSemaphore = new Semaphore(Math.max(1, numPrecomputingThread), true);

    _defaultModelCompletenessRequirements =
        MonitorUtils.combineLoadRequirementOptions(AnalyzerUtils.getGoalsByPriority(config));

    _loadMonitorTaskRunner =
        new LoadMonitorTaskRunner(config, _partitionMetricSampleAggregator, _brokerMetricSampleAggregator, _metadataClient,
                                  metricDef, time, dropwizardMetricRegistry, _brokerCapacityConfigResolver, executor);
    _clusterModelCreationTimer = dropwizardMetricRegistry.timer(MetricRegistry.name("LoadMonitor",
                                                                                    "cluster-model-creation-timer"));
    _loadMonitorExecutor = Executors.newScheduledThreadPool(2,
                                                            new KafkaCruiseControlThreadFactory("LoadMonitorExecutor", true, LOG));
    _loadMonitorExecutor.scheduleAtFixedRate(new SensorUpdater(), 0, SensorUpdater.UPDATE_INTERVAL_MS, TimeUnit.MILLISECONDS);
    _loadMonitorExecutor.scheduleAtFixedRate(new PartitionMetricSampleAggregatorCleaner(), 0,
                                             PartitionMetricSampleAggregatorCleaner.CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);
    dropwizardMetricRegistry.register(MetricRegistry.name("LoadMonitor", "valid-windows"),
                                      (Gauge<Integer>) this::numValidSnapshotWindows);
    dropwizardMetricRegistry.register(MetricRegistry.name("LoadMonitor", "monitored-partitions-percentage"),
                                      (Gauge<Double>) this::monitoredPartitionsPercentage);
    dropwizardMetricRegistry.register(MetricRegistry.name("LoadMonitor", "total-monitored-windows"),
                                      (Gauge<Integer>) this::totalMonitoredSnapshotWindows);
    dropwizardMetricRegistry.register(MetricRegistry.name("LoadMonitor", "num-partitions-with-extrapolations"),
                                      (Gauge<Integer>) this::numPartitionsWithExtrapolations);
  }


  /**
   * Start the load monitor.
   */
  public void startUp() {
    _loadMonitorTaskRunner.start(_config.getBoolean(SKIP_LOADING_SAMPLES_CONFIG));
  }

  /**
   * Shutdown the load monitor.
   */
  public void shutdown() {
    LOG.info("Shutting down load monitor.");
    try {
      _brokerCapacityConfigResolver.close();
      _topicConfigProvider.close();
      _loadMonitorExecutor.shutdown();
    } catch (Exception e) {
      LOG.warn("Received exception when closing broker capacity resolver.", e);
    }
    _loadMonitorTaskRunner.shutdown();
    _metadataClient.close();
    KafkaCruiseControlUtils.closeAdminClientWithTimeout(_adminClient);
    LOG.info("Load Monitor shutdown completed.");
  }

  /**
   * @return The state of the load monitor.
   */
  public LoadMonitorState state(OperationProgress operationProgress, Cluster kafkaCluster) {
    LoadMonitorTaskRunner.LoadMonitorTaskRunnerState state = _loadMonitorTaskRunner.state();
    int totalNumPartitions = MonitorUtils.totalNumPartitions(kafkaCluster);
    double minMonitoredPartitionsPercentage = _defaultModelCompletenessRequirements.minMonitoredPartitionsPercentage();

    // Get the window to monitored partitions percentage mapping.
    SortedMap<Long, Float> validPartitionRatio = _partitionMetricSampleAggregator.validPartitionRatioByWindows(kafkaCluster);

    // Get valid snapshot window number and populate the monitored partition map.
    SortedSet<Long> validWindows = _partitionMetricSampleAggregator.validWindows(kafkaCluster,
                                                                                 minMonitoredPartitionsPercentage);
    int numValidSnapshotWindows = validWindows.size();

    // Get the number of valid partitions and sample extrapolations.
    int numValidPartitions = 0;
    Map<TopicPartition, List<SampleExtrapolation>> extrapolations = Collections.emptyMap();
    if (_partitionMetricSampleAggregator.numAvailableWindows() >= _numPartitionMetricSampleWindows) {
      try {
        MetricSampleAggregationResult<String, PartitionEntity> metricSampleAggregationResult =
            _partitionMetricSampleAggregator.aggregate(kafkaCluster, Long.MAX_VALUE, operationProgress);
        Map<PartitionEntity, ValuesAndExtrapolations> loads = metricSampleAggregationResult.valuesAndExtrapolations();
        extrapolations = partitionSampleExtrapolations(metricSampleAggregationResult.valuesAndExtrapolations());
        numValidPartitions = loads.size();
      } catch (Exception e) {
        LOG.warn("Received exception when trying to get the load monitor state", e);
      }
    }

    switch (state) {
      case NOT_STARTED:
        return LoadMonitorState.notStarted();
      case RUNNING:
        return LoadMonitorState.running(numValidSnapshotWindows,
                                        validPartitionRatio,
                                        numValidPartitions,
                                        totalNumPartitions,
                                        extrapolations,
                                        _loadMonitorTaskRunner.reasonOfLatestPauseOrResume());
      case SAMPLING:
        return LoadMonitorState.sampling(numValidSnapshotWindows,
                                         validPartitionRatio,
                                         numValidPartitions,
                                         totalNumPartitions,
                                         extrapolations);
      case PAUSED:
        return LoadMonitorState.paused(numValidSnapshotWindows,
                                       validPartitionRatio,
                                       numValidPartitions,
                                       totalNumPartitions,
                                       extrapolations,
                                       _loadMonitorTaskRunner.reasonOfLatestPauseOrResume());
      case BOOTSTRAPPING:
        double bootstrapProgress = _loadMonitorTaskRunner.bootStrapProgress();
        // Handle the race between querying the state and getting the progress.
        return LoadMonitorState.bootstrapping(numValidSnapshotWindows,
                                              validPartitionRatio,
                                              numValidPartitions,
                                              totalNumPartitions,
                                              bootstrapProgress >= 0 ? bootstrapProgress : 1.0,
                                              extrapolations);
      case TRAINING:
        return LoadMonitorState.training(numValidSnapshotWindows,
                                         validPartitionRatio,
                                         numValidPartitions,
                                         totalNumPartitions,
                                         extrapolations);
      case LOADING:
        return LoadMonitorState.loading(numValidSnapshotWindows,
                                        validPartitionRatio,
                                        numValidPartitions,
                                        totalNumPartitions,
                                        _loadMonitorTaskRunner.sampleLoadingProgress());
      default:
        throw new IllegalStateException("Should never be here.");
    }
  }

  /**
   * @return The topic config provider.
   */
  public TopicConfigProvider topicConfigProvider() {
    return _topicConfigProvider;
  }

  /**
   * @return The load monitor task runner state.
   */
  public LoadMonitorTaskRunner.LoadMonitorTaskRunnerState taskRunnerState() {
    return _loadMonitorTaskRunner.state();
  }

  /**
   * Bootstrap the load monitor for a given period.
   * @param startMs the starting time of the bootstrap period.
   * @param endMs the end time of the bootstrap period.
   * @param clearMetrics clear the existing metric samples.
   */
  public void bootstrap(long startMs, long endMs, boolean clearMetrics) {
    _loadMonitorTaskRunner.bootstrap(startMs, endMs, clearMetrics);
  }

  /**
   * Bootstrap the load monitor from the given timestamp until it catches up.
   * This method clears all existing metric samples.
   * @param startMs the starting time of the bootstrap period.
   * @param clearMetrics clear the existing metric samples.
   */
  public void bootstrap(long startMs, boolean clearMetrics) {
    _loadMonitorTaskRunner.bootstrap(startMs, clearMetrics);
  }

  /**
   * Bootstrap the load monitor with the most recent metric samples until it catches up.
   * This method clears all existing metric samples.
   *
   * @param clearMetrics clear the existing metric samples.
   */
  public void bootstrap(boolean clearMetrics) {
    _loadMonitorTaskRunner.bootstrap(clearMetrics);
  }

  /**
   * Train the load model with metric samples.
   * @param startMs training period starting time.
   * @param endMs training period end time.
   */
  public void train(long startMs, long endMs) {
    _loadMonitorTaskRunner.train(startMs, endMs);
  }

  /**
   * @return The cluster information from Kafka metadata.
   */
  public Cluster kafkaCluster() {
    return _metadataClient.cluster();
  }

  /**
   * Pause all the activities of the load monitor. The load monitor can only be paused when it is in
   * RUNNING state.
   *
   * @param reason The reason for pausing metric sampling.
   */
  public void pauseMetricSampling(String reason) {
    _loadMonitorTaskRunner.pauseSampling(reason);
  }

  /**
   * Resume the activities of the load monitor.
   *
   * @param reason The reason for resuming metric sampling.
   */
  public void resumeMetricSampling(String reason) {
    _loadMonitorTaskRunner.resumeSampling(reason);
  }

  /**
   * Acquire the semaphore for the cluster model generation.
   * @param operationProgress the progress for the job.
   * @return A new auto closeable semaphore for the cluster model generation.
   * @throws InterruptedException
   */
  public AutoCloseableSemaphore acquireForModelGeneration(OperationProgress operationProgress)
      throws InterruptedException {
    if (_acquiredClusterModelSemaphore.get()) {
      throw new IllegalStateException("The thread has already acquired the semaphore for cluster model generation.");
    }
    WaitingForClusterModel step = new WaitingForClusterModel();
    operationProgress.addStep(step);
    _clusterModelSemaphore.acquire();
    _acquiredClusterModelSemaphore.set(true);
    step.done();
    return new AutoCloseableSemaphore();
  }

  /**
   * Get the latest metric values of the brokers. The metric values are from the current active metric window.
   *
   * @return The latest metric values of brokers.
   */
  public Map<BrokerEntity, ValuesAndExtrapolations> currentBrokerMetricValues() {
    return _brokerMetricSampleAggregator.peekCurrentWindow();
  }

  /**
   * Get the latest metric values of the partitions. The metric values are from the current active metric window.
   *
   * @return The latest metric values of partitions.
   */
  public Map<PartitionEntity, ValuesAndExtrapolations> currentPartitionMetricValues() {
    return _partitionMetricSampleAggregator.peekCurrentWindow();
  }

  /**
   * Get the most recent cluster load model before the given timestamp.
   *
   * @param now The current time in millisecond.
   * @param requirements the load requirements for getting the cluster model.
   * @param operationProgress the progress to report.
   * @return A cluster model with the configured number of windows whose timestamp is before given timestamp.
   * @throws NotEnoughValidWindowsException If there is not enough sample to generate cluster model.
   */
  public ClusterModel clusterModel(long now,
                                   ModelCompletenessRequirements requirements,
                                   OperationProgress operationProgress)
      throws NotEnoughValidWindowsException {
    ClusterModel clusterModel = clusterModel(DEFAULT_START_TIME_FOR_CLUSTER_MODEL, now, requirements, operationProgress);
    // Micro optimization: put the broker stats construction out of the lock.
    BrokerStats brokerStats = clusterModel.brokerStats(_config);
    // update the cached brokerLoadStats
    synchronized (this) {
      _cachedBrokerLoadStats = brokerStats;
      _cachedBrokerLoadGeneration = clusterModel.generation();
    }
    return clusterModel;
  }

  /**
   * Get the cluster load model for a time range.
   *
   * @param from start of the time window
   * @param to end of the time window
   * @param requirements the load completeness requirements.
   * @param operationProgress the progress of the job to report.
   * @return A cluster model with the available snapshots whose timestamp is in the given window.
   * @throws NotEnoughValidWindowsException If there is not enough sample to generate cluster model.
   */
  public ClusterModel clusterModel(long from,
                                   long to,
                                   ModelCompletenessRequirements requirements,
                                   OperationProgress operationProgress)
      throws NotEnoughValidWindowsException {
    return clusterModel(from, to, requirements, false, operationProgress);
  }

  /**
   * Get the cluster load model for a time range.
   *
   * @param from start of the time window
   * @param to end of the time window
   * @param requirements the load completeness requirements.
   * @param populateReplicaPlacementInfo whether populate replica placement information.
   * @param operationProgress the progress of the job to report.
   * @return A cluster model with the available snapshots whose timestamp is in the given window.
   * @throws NotEnoughValidWindowsException If there is not enough sample to generate cluster model.
   */
  public ClusterModel clusterModel(long from,
                                   long to,
                                   ModelCompletenessRequirements requirements,
                                   boolean populateReplicaPlacementInfo,
                                   OperationProgress operationProgress)
      throws NotEnoughValidWindowsException {
    long start = System.currentTimeMillis();

    MetadataClient.ClusterAndGeneration clusterAndGeneration = _metadataClient.refreshMetadata();
    Cluster cluster = clusterAndGeneration.cluster();

    // Get the metric aggregation result.
    MetricSampleAggregationResult<String, PartitionEntity> partitionMetricSampleAggregationResult =
        _partitionMetricSampleAggregator.aggregate(cluster, from, to, requirements, operationProgress);
    Map<PartitionEntity, ValuesAndExtrapolations> partitionValuesAndExtrapolations = partitionMetricSampleAggregationResult.valuesAndExtrapolations();
    GeneratingClusterModel step = new GeneratingClusterModel(partitionValuesAndExtrapolations.size());
    operationProgress.addStep(step);

    // Create an empty cluster model first.
    long currentLoadGeneration = partitionMetricSampleAggregationResult.generation();
    ModelGeneration modelGeneration = new ModelGeneration(clusterAndGeneration.generation(), currentLoadGeneration);
    MetricSampleCompleteness<String, PartitionEntity> completeness = partitionMetricSampleAggregationResult.completeness();
    ClusterModel clusterModel = new ClusterModel(modelGeneration, completeness.validEntityRatio());

    final Timer.Context ctx = _clusterModelCreationTimer.time();
    try {
      // Create the racks and brokers.
      // Shuffle nodes before getting their capacity from the capacity resolver.
      // This enables a capacity resolver to estimate the capacity of the nodes, for which the capacity retrieval has
      // failed.
      // The use case for this estimation is that if the capacity of one of the nodes is not available (e.g. due to some
      // 3rd party service issue), the capacity resolver may want to use the capacity of a peer node as the capacity for
      // that node.
      // To this end, Cruise Control handles the case that the first node is problematic so the capacity resolver does
      // not have the chance to get the capacity for the other nodes.
      // Shuffling the node order helps, as the problematic node is unlikely to always be the first node in the list.
      List<Node> shuffledNodes = new ArrayList<>(cluster.nodes());
      Collections.shuffle(shuffledNodes);
      for (Node node : shuffledNodes) {
        // If the rack is not specified, we use the host info as rack info.
        String rack = getRackHandleNull(node);
        clusterModel.createRack(rack);
        BrokerCapacityInfo brokerCapacity = _brokerCapacityConfigResolver.capacityForBroker(rack, node.host(), node.id());
        LOG.debug("Get capacity info for broker {}: total capacity {}, capacity by logdir {}.",
                  node.id(), brokerCapacity.capacity().get(Resource.DISK), brokerCapacity.diskCapacityByLogDir());
        clusterModel.createBroker(rack, node.host(), node.id(), brokerCapacity, populateReplicaPlacementInfo);
      }

      // Populate replica placement information for the cluster model if requested.
      Map<TopicPartition, Map<Integer, String>> replicaPlacementInfo = null;
      if (populateReplicaPlacementInfo) {
        replicaPlacementInfo = getReplicaPlacementInfo(clusterModel, cluster, _adminClient, _config);
      }

      // Populate snapshots for the cluster model.
      for (Map.Entry<PartitionEntity, ValuesAndExtrapolations> entry : partitionValuesAndExtrapolations.entrySet()) {
        TopicPartition tp = entry.getKey().tp();
        ValuesAndExtrapolations leaderLoad = entry.getValue();
        populatePartitionLoad(cluster, clusterModel, tp, leaderLoad, replicaPlacementInfo, _brokerCapacityConfigResolver);
        step.incrementPopulatedNumPartitions();
      }
      // Set the state of bad brokers in clusterModel based on the Kafka cluster state.
      setBadBrokerState(clusterModel, cluster);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Generated cluster model in {} ms", System.currentTimeMillis() - start);
      }
    } finally {
      ctx.stop();
    }
    return clusterModel;
  }

  /**
   * @return The current cluster model generation. This is useful to avoid unnecessary cluster model creation which is
   * expensive.
   */
  public ModelGeneration clusterModelGeneration() {
    int clusterGeneration = _metadataClient.refreshMetadata().generation();
    return new ModelGeneration(clusterGeneration, _partitionMetricSampleAggregator.generation());
  }

  /**
   * Get the cached load.
   * @return The cached load, or null if (1) load or metadata is stale or (2) cached load violates capacity requirements.
   */
  public synchronized BrokerStats cachedBrokerLoadStats(boolean allowCapacityEstimation) {
    if (_cachedBrokerLoadGeneration != null
        && (allowCapacityEstimation || !_cachedBrokerLoadStats.isBrokerStatsEstimated())
        && _partitionMetricSampleAggregator.generation() == _cachedBrokerLoadGeneration.loadGeneration()
        && _metadataClient.refreshMetadata().generation() == _cachedBrokerLoadGeneration.clusterGeneration()) {
      return _cachedBrokerLoadStats;
    }
    return null;
  }

  /**
   * Get all the active brokers in the cluster based on the replica assignment. If a metadata refresh failed due to
   * timeout, the current metadata information will be used. This is to handle the case that all the brokers are down.
   * @param timeout the timeout in milliseconds.
   * @return All the brokers in the cluster that has at least one replica assigned.
   */
  public Set<Integer> brokersWithReplicas(long timeout) {
    Cluster kafkaCluster = _metadataClient.refreshMetadata(timeout).cluster();
    return MonitorUtils.brokersWithReplicas(kafkaCluster);
  }

  /**
   * Refresh the cluster metadata and get the corresponding cluster and generation information.
   *
   * @return Cluster and generation information after refreshing the cluster metadata.
   */
  public MetadataClient.ClusterAndGeneration refreshClusterAndGeneration() {
    return _metadataClient.refreshMetadata();
  }

  /**
   * @return True if the monitored load meets the given completeness requirements, false otherwise.
   */
  public boolean meetCompletenessRequirements(Cluster cluster, ModelCompletenessRequirements requirements) {
    int numValidWindows =
        _partitionMetricSampleAggregator.validWindows(cluster, requirements.minMonitoredPartitionsPercentage()).size();
    int requiredNumValidWindows = requirements.minRequiredNumWindows();
    return numValidWindows >= requiredNumValidWindows;
  }

  /**
   * @return True if the monitored load meets the load requirements, false otherwise.
   */
  public boolean meetCompletenessRequirements(ModelCompletenessRequirements requirements) {
    MetadataClient.ClusterAndGeneration clusterAndGeneration = _metadataClient.refreshMetadata();
    return meetCompletenessRequirements(clusterAndGeneration.cluster(), requirements);
  }

  /**
   * @return All the available broker level metrics. Null is returned if nothing is available.
   */
  public MetricSampleAggregationResult<String, BrokerEntity> brokerMetrics() {
    List<Node> nodes = _metadataClient.cluster().nodes();
    Set<BrokerEntity> brokerEntities = new HashSet<>(nodes.size());
    for (Node node : nodes) {
      brokerEntities.add(new BrokerEntity(node.host(), node.id()));
    }
    return _brokerMetricSampleAggregator.aggregate(brokerEntities);
  }

  /**
   * Package private for unit test.
   */
  KafkaPartitionMetricSampleAggregator partitionSampleAggregator() {
    return _partitionMetricSampleAggregator;
  }

  /**
   * Get all the dead brokers in the cluster based on the replica assignment. If a metadata refresh failed due to
   * timeout, the current metadata information will be used. This is to handle the case that all the brokers are down.
   * @param timeout the timeout in milliseconds.
   * @return All the dead brokers which host some replicas in the cluster.
   */
  public Set<Integer> deadBrokersWithReplicas(long timeout) {
    Cluster kafkaCluster = _metadataClient.refreshMetadata(timeout).cluster();
    return MonitorUtils.deadBrokersWithReplicas(kafkaCluster);
  }

  /**
   * Get all the brokers having offline replca in the cluster based on the partition assignment. If a metadata refresh failed
   * due to timeout, the current metadata information will be used. This is to handle the case that all the brokers are down.
   * @param timeout the timeout in milliseconds.
   * @return All the brokers in the cluster that has at least one offline replica.
   */
  public Set<Integer> brokersWithOfflineReplicas(long timeout) {
    Cluster kafkaCluster = _metadataClient.refreshMetadata(timeout).cluster();
    return MonitorUtils.brokersWithOfflineReplicas(kafkaCluster);
  }

  private int numValidSnapshotWindows() {
    return _lastUpdate + SensorUpdater.UPDATE_TIMEOUT_MS > System.currentTimeMillis() ? _numValidSnapshotWindows : -1;
  }

  private int totalMonitoredSnapshotWindows() {
    return _lastUpdate + SensorUpdater.UPDATE_TIMEOUT_MS > System.currentTimeMillis() ? _totalMonitoredSnapshotWindows : -1;
  }

  private double monitoredPartitionsPercentage() {
    return _lastUpdate + SensorUpdater.UPDATE_TIMEOUT_MS > System.currentTimeMillis() ? _monitoredPartitionsPercentage : 0.0;
  }

  private int numPartitionsWithExtrapolations() {
    return _lastUpdate + SensorUpdater.UPDATE_TIMEOUT_MS > System.currentTimeMillis() ? _numPartitionsWithExtrapolations : -1;
  }

  private double getMonitoredPartitionsPercentage() {
    MetadataClient.ClusterAndGeneration clusterAndGeneration = _metadataClient.refreshMetadata();

    Cluster kafkaCluster = clusterAndGeneration.cluster();
    MetricSampleAggregationResult<String, PartitionEntity> metricSampleAggregationResult;
    try {
      metricSampleAggregationResult = _partitionMetricSampleAggregator.aggregate(kafkaCluster,
                                                                                 System.currentTimeMillis(),
                                                                                 new OperationProgress());
    } catch (NotEnoughValidWindowsException e) {
      return 0.0;
    }
    Map<PartitionEntity, ValuesAndExtrapolations> partitionLoads = metricSampleAggregationResult.valuesAndExtrapolations();
    AtomicInteger numPartitionsWithExtrapolations = new AtomicInteger(0);
    partitionLoads.values().forEach(valuesAndExtrapolations -> {
      if (!valuesAndExtrapolations.extrapolations().isEmpty()) {
        numPartitionsWithExtrapolations.incrementAndGet();
      }
    });
    _numPartitionsWithExtrapolations = numPartitionsWithExtrapolations.get();
    int totalNumPartitions = MonitorUtils.totalNumPartitions(kafkaCluster);
    return totalNumPartitions > 0 ? metricSampleAggregationResult.completeness().validEntityRatio() : 0.0;
  }

  /**
   * We have a separate class to update values for sensors.
   */
  private class SensorUpdater implements Runnable {
    // The interval for sensor value update.
    static final long UPDATE_INTERVAL_MS = 30000;
    // The maximum time allowed to make an update. If the sensor value cannot be updated in time, the sensor value
    // will be invalidated.
    static final long UPDATE_TIMEOUT_MS = 10 * UPDATE_INTERVAL_MS;

    @Override
    public void run() {
      try {
        MetadataClient.ClusterAndGeneration clusterAndGeneration = _metadataClient.clusterAndGeneration();
        double minMonitoredPartitionsPercentage = _defaultModelCompletenessRequirements.minMonitoredPartitionsPercentage();
        _numValidSnapshotWindows = _partitionMetricSampleAggregator.validWindows(clusterAndGeneration.cluster(),
                                                                                 minMonitoredPartitionsPercentage)
                                                                   .size();
        _monitoredPartitionsPercentage = getMonitoredPartitionsPercentage();
        _totalMonitoredSnapshotWindows = _partitionMetricSampleAggregator.allWindows().size();
        _lastUpdate = System.currentTimeMillis();
      } catch (Throwable t) {
        // We catch all the throwables because we don't want the sensor updater to die
        LOG.warn("Load monitor sensor updater received exception ", t);
      }
    }
  }

  /**
   * Background task to clean up the partition metric samples in case of topic deletion.
   *
   * Due to Kafka bugs, the returned metadata may not contain all the topics during broker bounce.
   * To handle that, we refresh metadata a few times and take a union of all the topics seen as the existing topics
   * -- in intervals of ({@link #CHECK_INTERVAL_MS} * {@link #REFRESH_LIMIT}).
   */
  private class PartitionMetricSampleAggregatorCleaner implements Runnable {
    static final long CHECK_INTERVAL_MS = 37500;
    static final short REFRESH_LIMIT = 8;
    // A set remember all the topics seen from last metadata refresh.
    private final Set<String> _allTopics = new HashSet<>();
    // The metadata refresh count.
    private int _refreshCount = 0;
    @Override
    public void run() {
      _allTopics.addAll(_metadataClient.refreshMetadata().cluster().topics());
      _refreshCount++;
      if (_refreshCount % REFRESH_LIMIT == 0) {
        _partitionMetricSampleAggregator.retainEntityGroup(_allTopics);
        _allTopics.clear();
      }
    }
  }

  public class AutoCloseableSemaphore implements AutoCloseable {
    private AtomicBoolean _closed = new AtomicBoolean(false);
    @Override
    public void close() {
      if (_closed.compareAndSet(false, true)) {
        _clusterModelSemaphore.release();
        _acquiredClusterModelSemaphore.set(false);
      }
    }
  }

}
