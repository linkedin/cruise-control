
This document contains some common exceptions, warnings and issues that might surface in the logs when trying to use Cruise Control (CC).

## Common Issues

- [ERROR _metricConsumer  returned null for _metricReporterTopic  __CruiseControlMetrics](#error-_metricconsumer-returned-null-for-_metricreportertopic-__cruisecontrolmetrics)
  - [Resolution](#resolution)
- [Timeout when Sampling Partition Metrics (in MetricFetchManager or MetricFetcher)](#timeout-during-partition-metric-sampling)
  - [Resolution](#resolution-1)
- [NotEnoughValidWindowsException - Not Enough Windows Available in Range](#notenoughvalidwindowsexception---not-enough-windows-available-in-range)
  - [Resolution](#resolution-2)
- [OptimizationFailureException: Self healing failed to move the replica away from decommissioned broker(s)](#optimizationfailureexception:-self-healing-failed-to-move-the-replica-away-from-decommissioned-broker(s))
  - [Resolution](#resolution-3)
- [WARN Skip generating metric sample for broker because all broker metrics are missing](#warn-skip-generating-metric-sample-for-broker-because-all-broker-metrics-are-missing)
  - [Resolution](#resolution-4)

### `ERROR _metricConsumer  returned null for _metricReporterTopic  __CruiseControlMetrics`

This means that Cruise Control could not find the `__CruiseControlMetrics` topic in the Kafka cluster. CC uses that topic (configured via [`metric.reporter.topic`](https://github.com/linkedin/cruise-control/blob/migrate_to_kafka_2_4/config/cruisecontrol.properties#L50)) to read broker metrics that help in generating the cluster model.

#### Resolution

1. Enable auto topic creation

 If auto creation of topics is disabled for your brokers, setting the [`auto.create.topics.enable`](https://kafka.apache.org/documentation/#brokerconfigs_auto.create.topics.enable) broker config to true will result in this metrics topic being created automatically, thereby resolving this error.

2. Create the topic manually

 If you do not want to set the `auto.create.topics.enable` config, then creating this topic manually is also an option. It is recommended to create the topic with the default replication factor of the cluster. Having this topic be a single-partition topic suffices.
 
### Timeout during partition metric sampling

The log lines for this error might look like the following:
```
[2021-02-12 04:37:09,424] INFO Kicking off partition metric sampling for time range [1613104599482, 1613104629424], duration 29942 ms with timeout 120000 ms. (com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricFetcherManager)
[2021-02-12 04:37:09,489] ERROR Received exception. (com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricFetcher)
org.apache.kafka.common.errors.TimeoutException: Failed to get offsets by times in 30000 ms
```

Cruise Control generates its cluster model using metrics in the `__CruiseControlMetrics` topic by default (overridable via the `metric.reporter.topic` config). It samples data in windows of time and errors out if it cannot find data in the topic for the given time range. So, in the above errors, there was no data in the metrics topic between the mentioned UNIX timestamps i.e. between Feb 11 2021 20:36:39 PST and Feb 11 2021 20:37:09 PST.

#### Resolution

1. If converting the mentioned UNIX timestamp results in a date in the very past or the very future, verify that the system clock is showing the right time, try restarting CC, and ensure that you are using the latest version of Cruise Control compatible with your version of Kafka (see [Environment Requirements](https://github.com/linkedin/cruise-control#environment-requirements)).
2. Ensure that the brokers are able to write to the `__CruiseControlMetrics` topic and that the partition(s) for this topic is/are not offline.
3. Try increasing the lookback window so that CC is able to find data older than the configured window range. The values of `num.partition.metrics.windows`, `partition.metrics.window.ms`, and `metric.sampling.interval.ms` can be increased to achieve this. An example [recommended](https://github.com/linkedin/cruise-control/issues/1472#issuecomment-783813331) config that collects 1-week history for partitions would be (1) `metric.sampling.interval.ms=300000`, (2) `partition.metrics.window.ms=3600000`, (3) `num.partition.metrics.windows=168`.

### `NotEnoughValidWindowsException` - Not Enough Windows Available in Range

The logs for this exception would look like:
```
Caused by: com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException: There is no window available in range [-1, 1600247526816] (index [1, -1]). Window index (current: 0, oldest: 0).
    at com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricSampleAggregator.aggregate(MetricSampleAggregator.java:202)
    at com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.KafkaPartitionMetricSampleAggregator.aggregate(KafkaPartitionMetricSampleAggregator.java:151)
    at com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor.clusterModel(LoadMonitor.java:541)
    at com.linkedin.kafka.cruisecontrol.KafkaCruiseControl.clusterModel(KafkaCruiseControl.java:326)
    at com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.LoadRunnable.clusterModel(LoadRunnable.java:99)
    ... 10 more
```

#### Resolution

This could be happening due to any of the following reasons:

1. Cruise Control instance has just been started up the first time (i.e. a cold start), and it hasn't had enough time to collect samples to generate a cluster model. If this is the case, give the CC instance some time (e.g. 5 - 10 minutes), or
2. A broker-side issue with producing metrics by Cruise Control metrics reporter to the relevant internal topic, or
3. A Cruise Control-side issue with consuming metrics from the relevant internal topic (e.g. `__CruiseControlMetrics`)

### `OptimizationFailureException`: Self healing failed to move the replica away from decommissioned broker(s)

A sample error from the CC logs might look like the following:
```
ERROR Error processing POST request '/remove_broker' due to: 'com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException: [ReplicaDistributionGoal] Self healing failed to move the replica Replica[isLeader=false,rack=sample_rack,broker=12345678,TopicPartition=sample_topic-0,origBroker=12345678] from decommissioned broker 12345677 (contains 1 replicas).'. (com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServlet)
```

#### Resolution

This error means that self-healing tried to move some replicas from the given broker(s) but could not do so. One reason for this might be that the replication factor of the topic is greater than the number of available brokers in the cluster. E.g. if the number of brokers in the cluster were 3 and the RF for this topic was also 3 and one of the brokers went down, then Cruise Control won't be able to move the replica to a dead brokers from the broker being decommissioned. That is, self-healing won't work in that case and we would have to ensure that we have enough brokers in the cluster to be able to satisfy the replication factor for the given partition at all times.

Another solution might be to reduce the replication factor for the topic but this should not be done without proper consideration.

### `WARN Skip generating metric sample for broker because all broker metrics are missing`

Sample log lines that accompany this warning might look like the following:
```
INFO Kicking off partition metric sampling for time range [1616448953094, 1616449073094], duration 120000 ms with timeout 120000 ms. (com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricFetcherManager)
INFO Finished sampling for topic partitions [__CruiseControlMetrics-0] in time range [1616448953094,1616449073094]. Collected 84318 metrics. (com.linkedin.kafka.cruisecontrol.monitor.sampling.CruiseControlMetricsReporterSampler)
WARN Skip generating metric sample for broker 2 because all broker metrics are missing. (com.linkedin.kafka.cruisecontrol.monitor.sampling.SamplingUtils)
WARN Skip generating metric sample for broker 0 because all broker metrics are missing. (com.linkedin.kafka.cruisecontrol.monitor.sampling.SamplingUtils)
WARN Skip generating metric sample for broker 1 because all broker metrics are missing. (com.linkedin.kafka.cruisecontrol.monitor.sampling.SamplingUtils)
INFO Generated 6333 partition metric samples and 3(3 skipped) broker metric samples for timestamp 1616449046081. (com.linkedin.kafka.cruisecontrol.monitor.sampling.CruiseControlMetricsProcessor)
INFO Collected 6333 partition metric samples for 6333 partitions. Total partition assigned: 6333. (com.linkedin.kafka.cruisecontrol.monitor.sampling.SamplingFetcher)
INFO Collected 3 broker metric samples for 3 brokers. (com.linkedin.kafka.cruisecontrol.monitor.sampling.SamplingFetcher)
INFO Finished sampling in 654 ms. (com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricFetcherManager)
```

This warning means that Cruise Control could not find broker metrics for the mentioned brokers (brokers `0`, `1`, `2`) in the `__CruiseControlMetrics` topic. It could, however, find broker metrics for 3 other brokers in the cluster. Without broker metrics, Cruise Control will not be able to generate a proper cluster model and might not be able to include the brokers with missing metrics in it poposal computations and task executions.

### Resolution

The general resolution is to ensure that the brokers with the missing metrics are able to produce metrics to the `__CruiseControlTopic`. Checking the following might help:

1. Ensure that the Cruise Control metrics reporter jar is present on the brokers
2. Ensure that you are using a version of Cruise Control compatible with the version of Kafka. For more information on version compatibility, see the [Environment Requirements](https://github.com/linkedin/cruise-control#environment-requirements) section

## Contributing

Found an error/interesting behaviour of Cruise Control and have a resolution for it? Consider adding that to this guide by following the [contributing guidelines](https://github.com/linkedin/cruise-control/blob/migrate_to_kafka_2_4/CONTRIBUTING.md) for the project.
