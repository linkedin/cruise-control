/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.common.MetadataClient;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigResolver;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.exception.ModelInputException;
import com.linkedin.kafka.cruisecontrol.exception.NotEnoughValidSnapshotsException;
import com.linkedin.kafka.cruisecontrol.exception.NotEnoughSnapshotsException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.MetricCompletenessChecker;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.MetricSampleAggregationResult;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.MetricSampleAggregator;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.PartitionMetricSample;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.Snapshot;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The LoadMonitor monitors the workload of a kafka cluster. It periodically triggers the metric sampling and
 * maintains the collected {@link PartitionMetricSample}. It is also responsible for aggregate the metrics samples into
 * {@link Snapshot} for the analyzer to generate the balancing proposals.
 */
public class LoadMonitor {

  // Kafka Load Monitor server log.
  private static final Logger LOG = LoggerFactory.getLogger(LoadMonitor.class);
  private static final long METADATA_TTL = 5000L;
  private final int _numSnapshots;
  private final LoadMonitorTaskRunner _loadMonitorTaskRunner;
  private final MetricSampleAggregator _metricSampleAggregator;
  // A semaphore to help throttle the simultaneous cluster model creation
  private final Semaphore _clusterModelSemaphore;
  private final MetadataClient _metadataClient;
  private final BrokerCapacityConfigResolver _brokerCapacityConfigResolver;
  private final ScheduledExecutorService _sensorUpdaterExecutor;
  private final MetricCompletenessChecker _metricCompletenessChecker;
  private final MetricRegistry _dropwizardMetricRegistry;
  private final Timer _clusterModelCreationTimer;
  private final ThreadLocal<Boolean> _acquiredClusterModelSemaphore;
  private final ModelCompletenessRequirements _defaultModelCompletenessRequirements;

  // Sensor values
  private volatile int _numValidSnapshotWindows;
  private volatile double _monitoredPartitionsPercentage;
  private volatile int _totalMonitoredSnapshotWindows;
  private volatile int _numPartitionsWithFlaw;
  private volatile long _lastUpdate;

  private volatile ModelGeneration _cachedBrokerLoadGeneration;
  private volatile ClusterModel.BrokerStats _cachedBrokerLoadStats;

  /**
   * Construct a load monitor.
   *
   * @param config The load monitor configuration.
   * @param time   The time object.
   * @param dropwizardMetricRegistry the sensor registry for cruise control
   */
  public LoadMonitor(KafkaCruiseControlConfig config,
                     Time time,
                     MetricRegistry dropwizardMetricRegistry) {
    this(config,
         new MetadataClient(config,
                            new Metadata(5000L, config.getLong(KafkaCruiseControlConfig.METADATA_MAX_AGE_CONFIG)),
                            METADATA_TTL,
                            time),
         time,
         dropwizardMetricRegistry);
  }

  /**
   * Package private constructor for unit tests.
   */
  LoadMonitor(KafkaCruiseControlConfig config,
              MetadataClient metadataClient,
              Time time,
              MetricRegistry dropwizardMetricRegistry) {
    _metadataClient = metadataClient;

    _brokerCapacityConfigResolver = config.getConfiguredInstance(KafkaCruiseControlConfig.BROKER_CAPACITY_CONFIG_RESOLVER_CLASS_CONFIG,
                                                                 BrokerCapacityConfigResolver.class);
    _numSnapshots = config.getInt(KafkaCruiseControlConfig.NUM_LOAD_SNAPSHOTS_CONFIG);

    _metricCompletenessChecker = new MetricCompletenessChecker(_numSnapshots);

    _metricSampleAggregator = new MetricSampleAggregator(config, metadataClient.metadata(), _metricCompletenessChecker);

    _acquiredClusterModelSemaphore = ThreadLocal.withInitial(() -> false);

    // We use the number of proposal precomputing threads config to ensure there is enough concurrency if users
    // wants that.
    int numPrecomputingThread = config.getInt(KafkaCruiseControlConfig.NUM_PROPOSAL_PRECOMPUTE_THREADS_CONFIG);
    _clusterModelSemaphore = new Semaphore(Math.max(1, numPrecomputingThread), true);

    _defaultModelCompletenessRequirements =
        MonitorUtils.combineLoadRequirementOptions(AnalyzerUtils.getGoalMapByPriority(config).values());

    _dropwizardMetricRegistry = dropwizardMetricRegistry;
    _loadMonitorTaskRunner = new LoadMonitorTaskRunner(config, _metricSampleAggregator, _metadataClient, time,
                                                       dropwizardMetricRegistry);
    _clusterModelCreationTimer = _dropwizardMetricRegistry.timer(MetricRegistry.name("LoadMonitor",
                                                                                     "cluster-model-creation-timer"));
    SensorUpdater sensorUpdater = new SensorUpdater();
    _sensorUpdaterExecutor = Executors.newSingleThreadScheduledExecutor(new KafkaCruiseControlThreadFactory("LoadMonitorSensorUpdater", true, LOG));
    _sensorUpdaterExecutor.scheduleAtFixedRate(sensorUpdater, 0, SensorUpdater.UPDATE_INTERVAL_MS, TimeUnit.MILLISECONDS);
    _dropwizardMetricRegistry.register(MetricRegistry.name("LoadMonitor", "valid-snapshot-windows"),
                                       (Gauge<Integer>) this::numValidSnapshotWindows);
    _dropwizardMetricRegistry.register(MetricRegistry.name("LoadMonitor", "monitored-partitions-percentage"),
                                       (Gauge<Double>) this::monitoredPartitionsPercentage);
    _dropwizardMetricRegistry.register(MetricRegistry.name("LoadMonitor", "total-monitored-snapshot-windows"),
                                       (Gauge<Integer>) this::totalMonitoredSnapshotWindows);
    _dropwizardMetricRegistry.register(MetricRegistry.name("LoadMonitor", "num-partitions-with-flaw"),
                                       (Gauge<Integer>) this::numPartitionsWithFlaw);
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
  public LoadMonitorState state() {
    LoadMonitorTaskRunner.LoadMonitorTaskRunnerState state = _loadMonitorTaskRunner.state();
    MetadataClient.ClusterAndGeneration clusterAndGeneration = _metadataClient.refreshMetadata();
    Cluster kafkaCluster = clusterAndGeneration.cluster();
    int totalNumPartitions = MonitorUtils.totalNumPartitions(kafkaCluster);
    double minMonitoredPartitionsPercentage = _defaultModelCompletenessRequirements.minMonitoredPartitionsPercentage();

    // Get the window to monitored partitions percentage mapping.
    Map<Long, Double> windowToMonitoredPercentage =
        _metricCompletenessChecker.monitoredPercentages(clusterModelGeneration(),
                                                        clusterAndGeneration.cluster(),
                                                        totalNumPartitions);

    // Get valid snapshot window number and populate the monitored partition map.
    // We do this primarily because the checker and aggregator are not always synchronized.
    int numValidSnapshotWindows = 0;
    boolean hasInvalidSnapshotWindows = false;
    SortedMap<Long, Double> loadSnapshotsWindows = new TreeMap<>(Comparator.reverseOrder());
    for (long window : _metricSampleAggregator.visibleSnapshotWindows()) {
      double monitoredPartitionsPercentage = windowToMonitoredPercentage.getOrDefault(window, 0.0);
      if (monitoredPartitionsPercentage >= minMonitoredPartitionsPercentage && !hasInvalidSnapshotWindows) {
        numValidSnapshotWindows++;
      } else {
        hasInvalidSnapshotWindows = true;
      }
      loadSnapshotsWindows.put(window, monitoredPartitionsPercentage);
    }

    // Get the number of valid partitions and sample flaws.
    int numValidPartitions = 0;
    Map<TopicPartition, List<MetricSampleAggregationResult.SampleFlaw>> sampleFlaws = Collections.emptyMap();
    if (_metricSampleAggregator.numSnapshotWindows() > _numSnapshots) {
      try {
        MetricSampleAggregationResult metricSampleAggregationResult =
            _metricSampleAggregator.recentSnapshots(kafkaCluster, Long.MAX_VALUE);
        Map<TopicPartition, Snapshot[]> loadSnapshots = metricSampleAggregationResult.snapshots();
        sampleFlaws = metricSampleAggregationResult.sampleFlaws();
        numValidPartitions = loadSnapshots.size();
      } catch (Exception e) {
        LOG.warn("Received exception when trying to get the load monitor state", e);
      }
    }

    switch (state) {
      case NOT_STARTED:
        return LoadMonitorState.notStarted();
      case RUNNING:
        return LoadMonitorState.running(numValidSnapshotWindows,
                                        loadSnapshotsWindows,
                                        numValidPartitions,
                                        totalNumPartitions,
                                        sampleFlaws);
      case SAMPLING:
        return LoadMonitorState.sampling(numValidSnapshotWindows,
                                         loadSnapshotsWindows,
                                         numValidPartitions,
                                         totalNumPartitions,
                                         sampleFlaws);
      case PAUSED:
        return LoadMonitorState.paused(numValidSnapshotWindows,
                                       loadSnapshotsWindows,
                                       numValidPartitions,
                                       totalNumPartitions,
                                       sampleFlaws);
      case BOOTSTRAPPING:
        double bootstrapProgress = _loadMonitorTaskRunner.bootStrapProgress();
        // Handle the race between querying the state and getting the progress.
        return LoadMonitorState.bootstrapping(numValidSnapshotWindows,
                                              loadSnapshotsWindows,
                                              numValidPartitions,
                                              totalNumPartitions,
                                              bootstrapProgress >= 0 ? bootstrapProgress : 1.0,
                                              sampleFlaws);
      case TRAINING:
        return LoadMonitorState.training(numValidSnapshotWindows,
                                         loadSnapshotsWindows,
                                         numValidPartitions,
                                         totalNumPartitions,
                                         sampleFlaws);
      case LOADING:
        return LoadMonitorState.loading(numValidSnapshotWindows,
                                        loadSnapshotsWindows,
                                        numValidPartitions,
                                        totalNumPartitions,
                                        _loadMonitorTaskRunner.sampleLoadingProgress());
      default:
        throw new IllegalStateException("Should never be here.");
    }
  }

  /**
   * Return the the load monitor task runner state.
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
   * @throws InterruptedException
   */
  public AutoCloseableSemaphore acquireForModelGeneration() throws InterruptedException {
    if (_acquiredClusterModelSemaphore.get()) {
      throw new IllegalStateException("The thread has already acquired the semaphore for cluster model generation.");
    }
    _clusterModelSemaphore.acquire();
    _acquiredClusterModelSemaphore.set(true);
    return new AutoCloseableSemaphore();
  }

  /**
   * Get the most recent cluster load model before the given timestamp.
   *
   * @param now The current time in millisecond.
   * @param requirements the load requirements for getting the cluster model.
   * @return A cluster model with the configured number of snapshots whose timestamp is before given timestamp.
   */
  public ClusterModel clusterModel(long now, ModelCompletenessRequirements requirements)
      throws ModelInputException, NotEnoughValidSnapshotsException, NotEnoughSnapshotsException {
    ClusterModel clusterModel = clusterModel(-1L, now, requirements);
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
   * Get the cluster load model for a time window. When there is
   *
   * @param from start of the time window
   * @param to end of the time window
   * @param requirements the load completeness requirements.
   * @return A cluster model with the available snapshots whose timestamp is in the given window.
   * @throws ModelInputException
   * @throws NotEnoughValidSnapshotsException
   * @throws NotEnoughSnapshotsException
   */
  public ClusterModel clusterModel(long from, long to, ModelCompletenessRequirements requirements)
      throws ModelInputException, NotEnoughValidSnapshotsException, NotEnoughSnapshotsException {
    long start = System.currentTimeMillis();

    MetadataClient.ClusterAndGeneration clusterAndGeneration = _metadataClient.refreshMetadata();
    Cluster kafkaCluster = clusterAndGeneration.cluster();
    int totalNumPartitions = MonitorUtils.totalNumPartitions(kafkaCluster);

    // Get the metric aggregation result.
    MetricSampleAggregationResult metricSampleAggregationResult =
        aggregateMetrics(from, to, clusterAndGeneration, requirements, totalNumPartitions);
    Map<TopicPartition, Snapshot[]> loadSnapshots = metricSampleAggregationResult.snapshots();

    // Create an empty cluster model first.
    long currentLoadGeneration = metricSampleAggregationResult.generation();
    ModelGeneration modelGeneration = new ModelGeneration(clusterAndGeneration.generation(), currentLoadGeneration);
    int numInvalidPartitions = requirements.includeAllTopics() ? metricSampleAggregationResult.invalidPartitions().size() : 0;
    double monitoredPercentage = (double) (loadSnapshots.size() - numInvalidPartitions) / totalNumPartitions;
    ClusterModel clusterModel = new ClusterModel(modelGeneration, monitoredPercentage);

    final Timer.Context ctx = _clusterModelCreationTimer.time();
    try {
      // Create the racks and brokers.
      for (Node node : kafkaCluster.nodes()) {
        // If the rack is not specified, we use the host info as rack info.
        String rack = getRackHandleNull(node);
        clusterModel.createRack(rack);
        Map<Resource, Double> brokerCapacity =
            _brokerCapacityConfigResolver.capacityForBroker(rack, node.host(), node.id());
        clusterModel.createBroker(rack, node.host(), node.id(), brokerCapacity);
      }

      // populate snapshots for the cluster model.
      for (Map.Entry<TopicPartition, Snapshot[]> entry : loadSnapshots.entrySet()) {
        TopicPartition tp = entry.getKey();
        Snapshot[] leaderLoadSnapshots = entry.getValue();
        populateSnapshots(kafkaCluster, clusterModel, tp, leaderLoadSnapshots);
      }

      // If the caller asks for all the topics and some of the partitions are missing, we need to fill empty
      // snapshots for them.
      if (requirements.includeAllTopics() && loadSnapshots.size() != totalNumPartitions) {
        fillInMissingPartitions(loadSnapshots, kafkaCluster, clusterModel);
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
    return new ModelGeneration(clusterGeneration, _metricSampleAggregator.currentGeneration());
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
          && _metricSampleAggregator.currentGeneration() == _cachedBrokerLoadGeneration.loadGeneration()) {
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
    int availableNumSnapshots = getAvailableNumSnapshots(clusterAndGeneration,
                                                         requirements.minMonitoredPartitionsPercentage(),
                                                         MonitorUtils.totalNumPartitions(clusterAndGeneration.cluster()));
    int requiredSnapshot = requirements.minRequiredNumSnapshotWindows();
    return availableNumSnapshots >= requiredSnapshot;
  }

  /**
   * Package private for unit test.
   */
  MetricSampleAggregator aggregator() {
    return _metricSampleAggregator;
  }

  /**
   * Package private for unit test.
   */
  MetricCompletenessChecker completenessChecker() {
    return _metricCompletenessChecker;
  }

  /**
   * Get the metric aggregation result from the metrics aggregator.
   *
   * The method is package private and static to facilitate testing..
   */
  private MetricSampleAggregationResult aggregateMetrics(long from,
                                                         long to,
                                                         MetadataClient.ClusterAndGeneration clusterAndGeneration,
                                                         ModelCompletenessRequirements requirements,
                                                         int totalNumPartitions)
      throws NotEnoughValidSnapshotsException, NotEnoughSnapshotsException {
    double minMonitoredPartitionsPercentage = requirements.minMonitoredPartitionsPercentage();
    int requiredNumSnapshotWindows = requirements.minRequiredNumSnapshotWindows();
    boolean includeAllTopics = requirements.includeAllTopics();

    // We need this while loop because it is possible that after we get an available number of partitions a new
    // window may be rolled out so we may get less valid partitions in the actually returned result. In this case
    // we will just retry, but this case should be rare.
    MetricSampleAggregationResult metricSampleAggregationResult = null;
    while (metricSampleAggregationResult == null) {
      int availableNumSnapshots = getAvailableNumSnapshots(clusterAndGeneration,
                                                           minMonitoredPartitionsPercentage,
                                                           totalNumPartitions);
      int numWindowsInRange = _metricCompletenessChecker.numWindows(from, to);
      availableNumSnapshots = Math.min(availableNumSnapshots, numWindowsInRange);
      if (availableNumSnapshots < requiredNumSnapshotWindows || numWindowsInRange < requiredNumSnapshotWindows) {
        throw new NotEnoughValidSnapshotsException(
            "The cluster model generation requires " + requiredNumSnapshotWindows + " snapshots with minimum " +
                "monitored partitions percentage of " + minMonitoredPartitionsPercentage
                + ". But there are only " + availableNumSnapshots + " valid snapshots available.");
      }

      metricSampleAggregationResult = _metricSampleAggregator.snapshots(clusterAndGeneration.cluster(),
                                                                        from,
                                                                        to,
                                                                        availableNumSnapshots,
                                                                        includeAllTopics);
      // Check if the monitored partition percentage is met. If not, we retry get the metric aggregation result.
      if (metricSampleAggregationResult.snapshots().size() < totalNumPartitions * minMonitoredPartitionsPercentage) {
        metricSampleAggregationResult = null;
      }
    }
    return metricSampleAggregationResult;
  }

  /**
   * Add empty load of all the partitions that exists in the current cluster but missing from the
   * metric aggregation result.
   */
  private void fillInMissingPartitions(Map<TopicPartition, Snapshot[]> loadSnapshots,
                                              Cluster kafkaCluster,
                                              ClusterModel clusterModel) throws ModelInputException {
    // There must be at least one entry, otherwise there will be exception thrown earlier. So we don't need to
    // check if it has next
    Snapshot[] snapshotsForTimestamps = loadSnapshots.values().iterator().next();
    Snapshot[] emptyLoadSnapshots = new Snapshot[snapshotsForTimestamps.length];
    for (int i = 0; i < emptyLoadSnapshots.length; i++) {
      emptyLoadSnapshots[i] = new Snapshot(snapshotsForTimestamps[i].time());
    }
    for (Node node : kafkaCluster.nodes()) {
      for (PartitionInfo partitionInfo : kafkaCluster.partitionsForNode(node.id())) {
        TopicPartition tp = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
        if (!loadSnapshots.containsKey(tp)) {
          populateSnapshots(kafkaCluster, clusterModel, tp, emptyLoadSnapshots);
        }
      }
    }
  }

  /**
   * Get the number of available snapshots which meets the cluster model completeness requirement.
   *
   * @param clusterAndGeneration The kafka cluster and metadata generation.
   * @param minMonitoredPartitionPercentage the required partition coverage.
   * @param totalNumPartitions the total number of partitions. We can actually get this from the cluster info. But
   *                           we choose to pass it in to void recalculate every time.
   * @return the number of available snapshots that meets the coverage requirement.
   */
  private int getAvailableNumSnapshots(MetadataClient.ClusterAndGeneration clusterAndGeneration,
                                      double minMonitoredPartitionPercentage,
                                      int totalNumPartitions) {
    ModelGeneration modelGeneration = new ModelGeneration(clusterAndGeneration.generation(),
                                                          _metricSampleAggregator.currentGeneration());
    Cluster kafkaCluster = clusterAndGeneration.cluster();
    return _metricCompletenessChecker.numValidWindows(modelGeneration,
                                                      kafkaCluster,
                                                      minMonitoredPartitionPercentage,
                                                      totalNumPartitions);
  }

  private void populateSnapshots(Cluster kafkaCluster,
                                 ClusterModel clusterModel,
                                 TopicPartition tp,
                                 Snapshot[] leaderLoadSnapshots) throws ModelInputException {
    PartitionInfo partitionInfo = kafkaCluster.partition(tp);
    // If partition info does not exist, the topic may have been deleted.
    if (partitionInfo != null) {
      for (Node replica : partitionInfo.replicas()) {
        boolean isLeader = partitionInfo.leader() != null && replica.id() == partitionInfo.leader().id();
        String rack = getRackHandleNull(replica);
        // If broker is dead, do not call resolver to get the capacity.
        Map<Resource, Double> brokerCapacity = kafkaCluster.nodeById(replica.id()) == null ? deadBrokerCapacity() :
            _brokerCapacityConfigResolver.capacityForBroker(rack, replica.host(), replica.id());
        clusterModel.createReplicaHandleDeadBroker(rack, replica.id(), tp, isLeader, brokerCapacity);
        // Push the load snapshot to the replica one by one.
        for (int i = 0; i < leaderLoadSnapshots.length; i++) {
          clusterModel.pushLatestSnapshot(rack, replica.id(), tp,
                                          isLeader ? leaderLoadSnapshots[i].duplicate() : MonitorUtils.toFollowerSnapshot(leaderLoadSnapshots[i]));
        }
      }
    }
  }

  private Map<Resource, Double> deadBrokerCapacity() {
    Map<Resource, Double> capacity = new HashMap<>();
    Resource.cachedValues().forEach(r -> capacity.put(r, -1.0));
    return capacity;
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

  private int numValidSnapshotWindows() {
    return _lastUpdate + SensorUpdater.UPDATE_TIMEOUT_MS > System.currentTimeMillis() ? _numValidSnapshotWindows : -1;
  }

  private int totalMonitoredSnapshotWindows() {
    return _lastUpdate + SensorUpdater.UPDATE_TIMEOUT_MS > System.currentTimeMillis() ? _totalMonitoredSnapshotWindows : -1;
  }

  private double monitoredPartitionsPercentage() {
    return _lastUpdate + SensorUpdater.UPDATE_TIMEOUT_MS > System.currentTimeMillis() ? _monitoredPartitionsPercentage : 0.0;
  }

  private int numPartitionsWithFlaw() {
    return _lastUpdate + SensorUpdater.UPDATE_TIMEOUT_MS > System.currentTimeMillis() ? _numPartitionsWithFlaw : -1;
  }

  private double getMonitoredPartitionsPercentage() {
    MetadataClient.ClusterAndGeneration clusterAndGeneration = _metadataClient.refreshMetadata();

    Cluster kafkaCluster = clusterAndGeneration.cluster();
    MetricSampleAggregationResult metricSampleAggregationResult;
    try {
      metricSampleAggregationResult = _metricSampleAggregator.recentSnapshots(kafkaCluster, System.currentTimeMillis());
    } catch (NotEnoughSnapshotsException e) {
      return 0.0;
    }
    Map<TopicPartition, Snapshot[]> loadSnapshots = metricSampleAggregationResult.snapshots();
    _numPartitionsWithFlaw = metricSampleAggregationResult.sampleFlaws().keySet().size();
    int totalNumPartitions = MonitorUtils.totalNumPartitions(kafkaCluster);
    return totalNumPartitions > 0 ? (double) loadSnapshots.size() / totalNumPartitions : 0.0;
  }

  /**
   * Basically a data dump of the current state of the metrics aggregator.
   * @return a non-null, unmodifiable map
   */
  public SortedMap<Long, Map<TopicPartition, Snapshot>> currentSnapshots() {
    return _metricSampleAggregator.currentSnapshots();
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
        int totalNumPartitions = MonitorUtils.totalNumPartitions(clusterAndGeneration.cluster());
        _numValidSnapshotWindows =
            getAvailableNumSnapshots(clusterAndGeneration, minMonitoredPartitionsPercentage, totalNumPartitions);
        _monitoredPartitionsPercentage = getMonitoredPartitionsPercentage();
        _totalMonitoredSnapshotWindows = _metricSampleAggregator.allSnapshotWindows().size();
        _lastUpdate = System.currentTimeMillis();
      } catch (Throwable t) {
        // We catch all the throwables because we don't want the sensor updater to die
        LOG.warn("Load monitor sensor updater received exception ", t);
      }
    }
  }

  public class AutoCloseableSemaphore implements AutoCloseable {
    @Override
    public void close() throws Exception {
      _clusterModelSemaphore.release();
      _acquiredClusterModelSemaphore.set(false);
    }
  }

}
