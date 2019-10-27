## Metric Sampler
The metric sampler is one of the most important pluggables in Kafka Cruise Control, it allows user to easily deploy Cruise Control to various environment and work with the existing metric system.

The default implementation of metric sampler is reading the broker metrics produced by `CruiseControlMetricsReporter` on the broker. This is assuming that users are running Kafka brokers by setting the `metric.reporters` configuration on the Kafka brokers to be `com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter`(see [quick start](https://github.com/linkedin/cruise-control#quick-start) on how to do that).

## Metric Sampler Partition Assignor
When users have multiple metric sampler threads, the metric sampler partition assignor is responsible for assign the partitions to the metric samplers. This is useful when users have some existing metric system. The default implementation assigns all the partitions of the same topic to the same metric sampler.

## Sample Store
The Sample Store is used to store the collected metric samples and training samples to external storage. One problem in metric sampling is that we are using some derived data from the raw metrics. And the way we derive the data relies on the metadata of the cluster at that moment. So when we look at the old metrics, if we do not know the metadata at the point the metric was collected the derived data would be inaccurate. Sample Store helps solve this problem by storing the derived data directly to an external storage for later loading.

The default implementation of Sample Store produces the samples back to the Kafka topic.

## Broker Capacity Config Resolver
The broker Capacity Config Resolver is the way for Cruise Control to get the broker capacity for each of the resources. The default implementation is file based properties. Users can also have a customized implementation to retrieve the capacity of the brokers from some hardware resource management system.

## Goals
The goals in Kafka Cruise Control are pluggable with different priorities. The default priority is:

* **Rack-awareness**: A goal that ensures all the replicas of each partition are assigned in a rack aware manner.

* **ReplicaCapacityGoal**: Attempt to make all the brokers in a cluster to have less than a given number of replicas.

* **CapacityGoals**: Goals that ensures the broker resource utilization is below a given threshold for the corresponding resource. Capacity goals are: 
    * DiskCapacityGoal
    * NetworkInboundCapacityGoal
    * NetworkOutboundCapacityGoal
    * CpuCapacityGoal

* **ReplicaDistributionGoal**: Attempt to make all the brokers in a cluster to have the similar number of replicas.

* **PotentialNwOutGoal**: A goal that ensures the potential network output (when all the replicas becomes leaders) on each of the broker do not exceed the broker’s network outbound bandwidth capacity.

* **ResourceDistributionGoals**: Attempt to make the resource utilization variance among all the brokers are within a certain range. This goal does not do anything if the cluster is in a low utilization mode (when all the resource utilization of each broker is below a configured percentage.) This is not a single goal, but consists of the following separate goals for each of the resources. 
    * DiskUtilDistributionGoal
    * NetworkInboundUtilDistributionGoal
    * NetworkOutboundUtilDistributionGoal
    * CpuUtilDistributionGoal

* **LeaderBytesInDistributionGoal**: Attempt to make the leader bytes in rate on each host to be balanced.

* **TopicReplicaDistributionGoal**: Attempt to make the replicas of the same topic are evenly distributed across the entire cluster.

* **LeaderReplicaDistributionGoal**: Attempt to make all the brokers in a cluster to have the similar number of leader replicas.

* **PreferredLeaderElectionGoal**: Attempt to make the first replica in replica list leader replica of the partition for all topic partition.

* **KafkaAssignerGoals**: These goals are used to make Cruise Control behaves like [Kafka assigner tool](https://github.com/linkedin/kafka-tools/wiki/Kafka-Assigner). These goals will be picked up if `kafka_assigner` parameter is set to true in corresponding request(e.g. [rebalance request](https://github.com/linkedin/cruise-control/wiki/REST-APIs#trigger-a-workload-balance)).
    * **KafkaAssignerDiskUsageDistributionGoal**: A goal that ensures all the replicas of each partition are assigned in a rack aware manner. 
    * **KafkaAssignerEvenRackAwareGoal**: Attempt to make all the brokers in a cluster to have the similar number of replicas.

* **IntraBrokerDiskCapacityGoal**: Goals that ensures the disk resource utilization is below a given threshold. This goal will be pick up if `rebalance_disk` parameter is set to `true` in [rebalance request](https://github.com/linkedin/cruise-control/wiki/REST-APIs#trigger-a-workload-balance). Not available in `kafka_0_11_and_1_0` branch. 

* **IntraBrokerDiskUsageDistributionGoal**: Attempt to make the utilization variance among all the disks within same broker are within a certain range. This goal will be pick up if `rebalance_disk` parameter is set to `true` in [rebalance request](https://github.com/linkedin/cruise-control/wiki/REST-APIs#trigger-a-workload-balance). Not available in `kafka_0_11_and_1_0` branch. 

## Anomaly Notifier
The anomaly notifier is a communication channel between cruise control and users. It notify the users about the anomaly detected in the cluster and let users make decision on what action to take about the anomaly. The anomalies include:
* Broker failure
* Goal violation
* Metric Anomaly
* Disk failure (not available in `kafka_0_11_and_1_0` branch)

The actions users can take are:
* Fix the anomaly
* Wait some time and check if the anomaly still exists
* Ignore the anomaly

By default Cruise Control is configured to use `NoopNotifier` which ignores all the anomalies.

## Replica Movement Strategy
The strategy determine the execution order for generated proposals. By default `BaseReplicaMovementStrategy` is used, which is totally random. Sometimes this could result in prolonged execution time due to some long tail tasks in each execution batches. Other available strategies includes:
* **PrioritizeSmallReplicaMovementStrategy**: first move small sized replicas
* **PrioritizeLargeReplicaMovementStrategy**: first move large sized replicas
* **PostponeUrpReplicaMovementStrategy**: first move replicas for partition having no out-of-sync replica

The strategies can be chained to use and can be dynamically set using `replica_movement_strategies` in corresponding request(e.g. [rebalance request](https://github.com/linkedin/cruise-control/wiki/REST-APIs#trigger-a-workload-balance)).
