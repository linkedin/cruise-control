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
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.Extrapolation;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricSampleCompleteness;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.common.MetadataClient;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigResolver;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.async.progress.GeneratingClusterModel;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.async.progress.WaitingForClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.PartitionEntity;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.KafkaPartitionMetricSampleAggregator;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricSampleAggregationResult;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.PartitionMetricSample;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.SampleExtrapolation;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The LoadMonitor monitors the workload of a Kafka cluster. It periodically triggers the metric sampling and
 * maintains the collected {@link PartitionMetricSample}. It is also responsible for aggregate the metrics samples into
 * {@link AggregatedMetricValues} for the analyzer to generate the balancing proposals.
 */
public class LoadMonitor {

  // Kafka Load Monitor server log.
  private static final Logger LOG = LoggerFactory.getLogger(LoadMonitor.class);
  private static final long METADATA_TTL = 5000L;
  private final int _numWindows;
  private final LoadMonitorTaskRunner _loadMonitorTaskRunner;
  private final KafkaPartitionMetricSampleAggregator _metricSampleAggregator;
  // A semaphore to help throttle the simultaneous cluster model creation
  private final Semaphore _clusterModelSemaphore;
  private final MetadataClient _metadataClient;
  private final BrokerCapacityConfigResolver _brokerCapacityConfigResolver;
  private final ScheduledExecutorService _sensorUpdaterExecutor;
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
  private volatile ClusterModel.BrokerStats _cachedBrokerLoadStats;

  /**
   * Construct a load monitor.
   *
   * @param config The load monitor configuration.
   * @param time   The time object.
   * @param dropwizardMetricRegistry The sensor registry for cruise control
   * @param metricDef The metric definitions.
   */
  public LoadMonitor(KafkaCruiseControlConfig config,
                     Time time,
                     MetricRegistry dropwizardMetricRegistry,
                     MetricDef metricDef) {
    this(config,
         new MetadataClient(config,
                            new Metadata(5000L, config.getLong(KafkaCruiseControlConfig.METADATA_MAX_AGE_CONFIG)),
                            METADATA_TTL,
                            time),
         time,
         dropwizardMetricRegistry,
         metricDef);
  }

  /**
   * Package private constructor for unit tests.
   */
  LoadMonitor(KafkaCruiseControlConfig config,
              MetadataClient metadataClient,
              Time time,
              MetricRegistry dropwizardMetricRegistry,
              MetricDef metricDef) {
    _metadataClient = metadataClient;

    _brokerCapacityConfigResolver = config.getConfiguredInstance(KafkaCruiseControlConfig.BROKER_CAPACITY_CONFIG_RESOLVER_CLASS_CONFIG,
                                                                 BrokerCapacityConfigResolver.class);
    _numWindows = config.getInt(KafkaCruiseControlConfig.NUM_METRICS_WINDOWS_CONFIG);

    _metricSampleAggregator = new KafkaPartitionMetricSampleAggregator(config, metadataClient.metadata());

    _acquiredClusterModelSemaphore = ThreadLocal.withInitial(() -> false);
    
    // We use the number of proposal precomputing threads config to ensure there is enough concurrency if users
    // wants that.
    int numPrecomputingThread = config.getInt(KafkaCruiseControlConfig.NUM_PROPOSAL_PRECOMPUTE_THREADS_CONFIG);
    _clusterModelSemaphore = new Semaphore(Math.max(1, numPrecomputingThread), true);

    _defaultModelCompletenessRequirements =
        MonitorUtils.combineLoadRequirementOptions(AnalyzerUtils.getGoalMapByPriority(config).values());

    _loadMonitorTaskRunner = new LoadMonitorTaskRunner(config, _metricSampleAggregator, _metadataClient, metricDef,
                                                       time, dropwizardMetricRegistry);
    _clusterModelCreationTimer = dropwizardMetricRegistry.timer(MetricRegistry.name("LoadMonitor",
                                                                                     "cluster-model-creation-timer"));
    SensorUpdater sensorUpdater = new SensorUpdater();
    _sensorUpdaterExecutor = Executors.newSingleThreadScheduledExecutor(new KafkaCruiseControlThreadFactory("LoadMonitorSensorUpdater", true, LOG));
    _sensorUpdaterExecutor.scheduleAtFixedRate(sensorUpdater, 0, SensorUpdater.UPDATE_INTERVAL_MS, TimeUnit.MILLISECONDS);
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
    _loadMonitorTaskRunner.start();
  }

  /**
   * Shutdown the load monitor.
   */
  public void shutdown() {
    LOG.info("Shutting down load monitor.");
    try {
      _brokerCapacityConfigResolver.close();
      _sensorUpdaterExecutor.shutdown();
    } catch (Exception e) {
      LOG.warn("Received exception when closing broker capacity resolver.", e);
    }
    _loadMonitorTaskRunner.shutdown();
    LOG.info("Load Monitor shutdown completed.");
  }

  /**
   * Get the state of the load monitor.
   */
  public LoadMonitorState state(OperationProgress operationProgress) {
    LoadMonitorTaskRunner.LoadMonitorTaskRunnerState state = _loadMonitorTaskRunner.state();
    MetadataClient.ClusterAndGeneration clusterAndGeneration = _metadataClient.refreshMetadata();
    Cluster kafkaCluster = clusterAndGeneration.cluster();
    int totalNumPartitions = MonitorUtils.totalNumPartitions(kafkaCluster);
    double minMonitoredPartitionsPercentage = _defaultModelCompletenessRequirements.minMonitoredPartitionsPercentage();

    // Get the window to monitored partitions percentage mapping.
    SortedMap<Long, Float> validPartitionRatio =
        _metricSampleAggregator.partitionCoverageByWindows(clusterAndGeneration);

    // Get valid snapshot window number and populate the monitored partition map.
    // We do this primarily because the checker and aggregator are not always synchronized.
    SortedSet<Long> validWindows = _metricSampleAggregator.validWindows(clusterAndGeneration,
                                                                        minMonitoredPartitionsPercentage);
    int numValidSnapshotWindows = validWindows.size();

    // Get the number of valid partitions and sample extrapolations.
    int numValidPartitions = 0;
    Map<TopicPartition, List<SampleExtrapolation>> extrapolations = Collections.emptyMap();
    if (_metricSampleAggregator.numAvailableWindows() >= _numWindows) {
      try {
        MetricSampleAggregationResult<String, PartitionEntity> metricSampleAggregationResult =
            _metricSampleAggregator.aggregate(clusterAndGeneration, Long.MAX_VALUE, operationProgress);
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
                                        extrapolations);
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
                                       extrapolations);
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
   * Return the load monitor task runner state.
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
   * Pause all the activities of the load monitor. The load monitor can only be paused when it is in
   * RUNNING state.
   */
  public void pauseMetricSampling() {
    _loadMonitorTaskRunner.pauseSampling();
  }

  /**
   * Resume the activities of the load monitor.
   */
  public void resumeMetricSampling() {
    _loadMonitorTaskRunner.resumeSampling();
  }

  /**
   * Acquire the semaphore for the cluster model generation.
   * @param operationProgress the progress for the job.
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
   * Get the most recent cluster load model before the given timestamp.
   *
   * @param now The current time in millisecond.
   * @param requirements the load requirements for getting the cluster model.
   * @param operationProgress the progress to report.
   * @return A cluster model with the configured number of windows whose timestamp is before given timestamp.
   */
  public ClusterModel clusterModel(long now, ModelCompletenessRequirements requirements, OperationProgress operationProgress)
      throws NotEnoughValidWindowsException {
    ClusterModel clusterModel = clusterModel(-1L, now, requirements, operationProgress);
    // Micro optimization: put the broker stats construction out of the lock.
    ClusterModel.BrokerStats brokerStats = clusterModel.brokerStats();
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
   * @throws NotEnoughValidWindowsException
   */
  public ClusterModel clusterModel(long from,
                                   long to,
                                   ModelCompletenessRequirements requirements,
                                   OperationProgress operationProgress)
      throws NotEnoughValidWindowsException {
    long start = System.currentTimeMillis();

    MetadataClient.ClusterAndGeneration clusterAndGeneration = _metadataClient.refreshMetadata();
    Cluster kafkaCluster = clusterAndGeneration.cluster();

    // Get the metric aggregation result.
    MetricSampleAggregationResult<String, PartitionEntity> metricSampleAggregationResult =
        _metricSampleAggregator.aggregate(clusterAndGeneration, from, to, requirements, operationProgress);
    Map<PartitionEntity, ValuesAndExtrapolations> loadSnapshots = metricSampleAggregationResult.valuesAndExtrapolations();
    GeneratingClusterModel step = new GeneratingClusterModel(loadSnapshots.size());
    operationProgress.addStep(step);

    // Create an empty cluster model first.
    long currentLoadGeneration = metricSampleAggregationResult.generation();
    ModelGeneration modelGeneration = new ModelGeneration(clusterAndGeneration.generation(), currentLoadGeneration);
    MetricSampleCompleteness<String, PartitionEntity> completeness = metricSampleAggregationResult.completeness();
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
      List<Node> shuffledNodes = new ArrayList<>(kafkaCluster.nodes());
      Collections.shuffle(shuffledNodes);
      for (Node node : shuffledNodes) {
        // If the rack is not specified, we use the host info as rack info.
        String rack = getRackHandleNull(node);
        clusterModel.createRack(rack);
        Map<Resource, Double> brokerCapacity =
            _brokerCapacityConfigResolver.capacityForBroker(rack, node.host(), node.id());
        clusterModel.createBroker(rack, node.host(), node.id(), brokerCapacity);
      }

      // populate snapshots for the cluster model.
      for (Map.Entry<PartitionEntity, ValuesAndExtrapolations> entry : loadSnapshots.entrySet()) {
        TopicPartition tp = entry.getKey().tp();
        ValuesAndExtrapolations leaderLoad = entry.getValue();
        populateSnapshots(kafkaCluster, clusterModel, tp, leaderLoad);
      }

      // Get the dead brokers and mark them as dead.
      deadBrokers(kafkaCluster).forEach(brokerId -> clusterModel.setBrokerState(brokerId, Broker.State.DEAD));
      LOG.debug("Generated cluster model in {} ms", System.currentTimeMillis() - start);
    } finally {
      ctx.stop();
    }
    return clusterModel;
  }

  /**
   * Get the current cluster model generation. This is useful to avoid unnecessary cluster model creation which is
   * expensive.
   */
  public ModelGeneration clusterModelGeneration() {
    int clusterGeneration = _metadataClient.refreshMetadata().generation();
    return new ModelGeneration(clusterGeneration, _metricSampleAggregator.generation());
  }

  /**
   * Get the cached load.
   * @return the cached load, null if the load
   */
  public ClusterModel.BrokerStats cachedBrokerLoadStats() {
    int clusterGeneration = _metadataClient.refreshMetadata().generation();
    synchronized (this) {
      if (_cachedBrokerLoadGeneration != null
          && clusterGeneration == _cachedBrokerLoadGeneration.clusterGeneration()
          && _metricSampleAggregator.generation() == _cachedBrokerLoadGeneration.loadGeneration()) {
        return _cachedBrokerLoadStats;
      }
    }
    return null;
  }

  /**
   * Get all the active brokers in the cluster based on the partition assignment. If a metadata refresh failed due to
   * timeout, the current metadata information will be used. This is to handle the case that all the brokers are down.
   * @param timeout the timeout in milliseconds.
   * @return All the brokers in the cluster that has at least one replica assigned.
   */
  public Set<Integer> brokersWithPartitions(long timeout) {
    Cluster kafkaCluster = _metadataClient.refreshMetadata(timeout).cluster();
    return brokersWithPartitions(kafkaCluster);
  }

  /**
   * Check whether the monitored load meets the load requirements.
   */
  public boolean meetCompletenessRequirements(ModelCompletenessRequirements requirements) {
    MetadataClient.ClusterAndGeneration clusterAndGeneration = _metadataClient.refreshMetadata();
    int availableNumSnapshots =
        _metricSampleAggregator.validWindows(clusterAndGeneration,
                                             requirements.minMonitoredPartitionsPercentage())
                               .size();
    int requiredSnapshot = requirements.minRequiredNumWindows();
    return availableNumSnapshots >= requiredSnapshot;
  }

  /**
   * Package private for unit test.
   */
  KafkaPartitionMetricSampleAggregator aggregator() {
    return _metricSampleAggregator;
  }

  private void populateSnapshots(Cluster kafkaCluster,
                                 ClusterModel clusterModel,
                                 TopicPartition tp,
                                 ValuesAndExtrapolations valuesAndExtrapolations) {
    PartitionInfo partitionInfo = kafkaCluster.partition(tp);
    // If partition info does not exist, the topic may have been deleted.
    if (partitionInfo != null) {
      for (int index = 0; index < partitionInfo.replicas().length; index++) {
        Node replica = partitionInfo.replicas()[index];
        boolean isLeader;
        if (partitionInfo.leader() == null) {
          LOG.warn("Detected offline partition {}-{}, skipping", partitionInfo.topic(), partitionInfo.partition());
          continue;
        } else {
          isLeader = replica.id() == partitionInfo.leader().id();
        }
        String rack = getRackHandleNull(replica);
        // Note that we assume the capacity resolver can still return the broker capacity even if the broker
        // is dead. We need this to get the host resource capacity.
        Map<Resource, Double> brokerCapacity =
            _brokerCapacityConfigResolver.capacityForBroker(rack, replica.host(), replica.id());
        clusterModel.createReplicaHandleDeadBroker(rack, replica.id(), tp, index, isLeader, brokerCapacity);
        AggregatedMetricValues aggregatedMetricValues = valuesAndExtrapolations.metricValues();
        clusterModel.setReplicaLoad(rack, replica.id(), tp,
                                    isLeader ? aggregatedMetricValues : MonitorUtils.toFollowerMetricValues(aggregatedMetricValues),
                                    valuesAndExtrapolations.windows());
      }
    }
  }

  private String getRackHandleNull(Node node) {
    return node.rack() == null || node.rack().isEmpty() ? node.host() : node.rack();
  }

  private Set<Integer> brokersWithPartitions(Cluster kafkaCluster) {
    Set<Integer> allBrokers = new HashSet<>();
    for (String topic : kafkaCluster.topics()) {
      for (PartitionInfo pi : kafkaCluster.partitionsForTopic(topic)) {
        for (Node node : pi.replicas()) {
          allBrokers.add(node.id());
        }
      }
    }
    return allBrokers;
  }

  private Set<Integer> deadBrokers(Cluster kafkaCluster) {
    Set<Integer> brokersWithPartitions = brokersWithPartitions(kafkaCluster);
    kafkaCluster.nodes().forEach(node -> brokersWithPartitions.remove(node.id()));
    return brokersWithPartitions;
  }

  private Map<TopicPartition, List<SampleExtrapolation>> partitionSampleExtrapolations(Map<PartitionEntity, ValuesAndExtrapolations> valuesAndExtrapolations) {
    Map<TopicPartition, List<SampleExtrapolation>> sampleExtrapolations = new HashMap<>();
    for (Map.Entry<PartitionEntity, ValuesAndExtrapolations> entry : valuesAndExtrapolations.entrySet()) {
      TopicPartition tp = entry.getKey().tp();
      Map<Integer, Extrapolation> extrapolations = entry.getValue().extrapolations();
      if (extrapolations.isEmpty()) {
        List<SampleExtrapolation> extrapolationForPartition = sampleExtrapolations.computeIfAbsent(tp, p -> new ArrayList<>());
        extrapolations.forEach((t, imputation) -> extrapolationForPartition.add(new SampleExtrapolation(t, imputation)));
      }
    }
    return sampleExtrapolations;
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
      metricSampleAggregationResult = _metricSampleAggregator.aggregate(clusterAndGeneration,
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
        _numValidSnapshotWindows = _metricSampleAggregator.validWindows(clusterAndGeneration,
                                                                        minMonitoredPartitionsPercentage)
                                                          .size();
        _monitoredPartitionsPercentage = getMonitoredPartitionsPercentage();
        _totalMonitoredSnapshotWindows = _metricSampleAggregator.allWindows().size();
        _lastUpdate = System.currentTimeMillis();
      } catch (Throwable t) {
        // We catch all the throwables because we don't want the sensor updater to die
        LOG.warn("Load monitor sensor updater received exception ", t);
      }
    }
  }

  public class AutoCloseableSemaphore implements AutoCloseable {
    private AtomicBoolean _closed = new AtomicBoolean(false);
    @Override
    public void close() throws Exception {
      if (_closed.compareAndSet(false, true)) {
        _clusterModelSemaphore.release();
        _acquiredClusterModelSemaphore.set(false);
      }
    }
  }

}
