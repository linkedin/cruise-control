Cruise Control for Apache Kafka
===================

### Introduction ###
  Cruise Control is a product that helps run Apache Kafka clusters at large scale. Due to the popularity of 
  Apache Kafka, many companies have bigger and bigger Kafka clusters. At LinkedIn, we have 1800+ Kafka brokers, 
  which means broker deaths are an almost daily occurrence and balancing the workload of Kafka also becomes a big overhead. 
  
  Kafka Cruise control is designed to address this operation scalability issue.
  
### Features ###
  Kafka Cruise Control provides the following features out of the box:
  
  * Resource utilization tracking for brokers, topics, and partitions.
  
  * Multi-goal rebalance proposal generation for:
    * Rack-awareness
    * Resource utilization balance (CPU, DISK, Network I/O)
    * Leader traffic distribution
    * Replica distribution for topics
    * Global replica distribution
    * Custom goals that you wrote and plugged in
  
  * Anomaly detection and alerting for the Kafka cluster, including:
    * Goal violation
    * Broker failure detection
  
  * Admin operations, including:
    * Add brokers
    * Decommission brokers
    * Rebalance the cluster

### Environment Requirements
* The current master branch of Cruise Control is compatible with Apache Kafka 0.10.1 and above
* message.format.version 0.10.0 and above is needed
* The master branch compiles with Scala 2.11

### Quick Start ###
0. This step is required if `CruiseControlMetricsReporter` is used for metrics collection (this is the default config
for Cruise Control). The metrics reporter periodically samples the Kafka raw metrics on the broker and sends them 
to a Kafka topic.
    * ```./gradlew jar```
    * Copy `./cruise-control-metrics-reporter/build/libs/cruise-control-metrics-reporter.jar` to your Kafka server 
    dependency jar folder. For Apache Kafka, the folder would be `core/build/dependant-libs-SCALA_VERSION/`
    * Modify Kafka server configuration to set `metric.reporters` to 
    `com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter`. For Apache Kafka, server properties
    are located at `./config/server.properties`.
    * Start ZooKeeper and Kafka server.
1. Modify config/cruisecontrol.properties to 
    * fill in `bootstrap.servers` and `zookeeper.connect` to the Kafka cluster to be monitored.
    * set `metric.sampler.class` to your implementation (the default sampler class is CruiseControlMetricsReporterSampler) 
    * set `sample.store.class` to your implementation if necessary (the default SampleStore is KafkaSampleStore)
2. Run the following command 
    ```
    ./gradlew jar copyDependantLibs
    ./kafka-cruise-control-start.sh [-jars PATH_TO_YOUR_JAR_1,PATH_TO_YOUR_JAR_2] config/cruisecontrol.properties [port]
    ```
    JAR files correspond to your applications and `port` enables customizing the cruise control port number (default: 9090).
3. visit http://localhost:9090/kafkacruisecontrol/state or http://localhost:\[port\]/kafkacruisecontrol/state if 
you specified the port when starting cruise control. 

**Note**: 
* Cruise Control will need some time to read the raw Kafka metrics from the cluster.
* The metrics of a newly up broker may take a few minutes to get stable. Cruise Control will drop the inconsistent 
metrics (e.g when topic bytes-in is higher than broker bytes-in), so first few snapshot windows may not have enough valid partitions. 

### REST API ###
Cruise Control provides a [REST API](https://github.com/linkedin/cruise-control/wiki/REST-APIs) for users 
to interact with. See the wiki page for more details.

### How Does It Work ###
Cruise Control relies on the recent load information of replicas to optimize the cluster.

Cruise Control periodically collects resource utilization samples at both broker- and partition-level to 
infer the traffic pattern of each partition. Based on the traffic characteristics of all the partitions, 
it derives the load impact of each partition over the brokers. Cruise Control then builds a workload
model to simulate the workload of the Kafka cluster. The goal optimizer explores different ways to generate 
cluster workload optimization proposals based on the user-specified list of goals.

Cruise Control also monitors the liveness of all the brokers in the cluster. 
To avoid the loss of redundancy, Cruise Control automatically moves replicas from failed brokers to healthy ones.

For more details about how Cruise Control achieves that, see 
[these slides](https://www.slideshare.net/JiangjieQin/introduction-to-kafka-cruise-control-68180931).

### Configurations for Cruise Control ###
To read more about the configurations. Check the 
[configurations wiki page](https://github.com/linkedin/cruise-control/wiki/Configurations).

### Pluggable Components ###
More about pluggable components can be found in the 
[pluggable components wiki page](https://github.com/linkedin/cruise-control/wiki/Pluggable-Components).

#### Metric Sampler #### 
The metric sampler enables users to deploy Cruise Control to various environments and work with the existing metric systems.

Cruise Control provides a metrics reporter that can be configured in your Apache Kafka server. Metrics reporter generates
performance metrics to a kafka metrics topic that can be consumed by Cruise Control.

#### Sample Store ####
The Sample Store enables storage of collected metric samples and training samples in an external storage. 

Metric sampling uses derived data from the raw metrics, and the accuracy of the derived data depends on the metadata of the cluster at that point.
Hence, when we look at the old metrics, if we do not know the metadata at the point the metric was collected, the derived data would not be accurate.
Sample Store helps solving this problem by storing the derived data directly to an external storage for later loading.

The default Sample Store implementation produces metric samples back to Kafka.

#### Goals ####
The goals in Cruise Control are pluggable with different priorities. The default goals in order of decreasing priority are:
 * **RackAwareCapacityGoal** - Ensures that (1) all replicas of each partition are assigned in a rack aware manner -- i.e. no more than one replica of 
 each partition resides in the same rack, (2) for each resource, the utilization of each broker is below a given threshold.
 * **PotentialNwOutGoal** - Ensures that the potential network output (when all the replicas in the broker become leaders) on each of the broker do 
 not exceed the broker’s network outbound bandwidth capacity.
 * **ResourceDistributionGoal** - Attempts to keep the workload variance among brokers within a certain range relative to the average utilization of each resource.
 * **LeaderBytesInDistributsionGoal** - Attempts to equalize the leader bytes in rate on each host.
 * **TopicReplicaDistributionGoal** - Attempts to maintain an even distribution of any topic's replicas across the entire cluster.
 * **ReplicaDistributionGoal** - Attempts to make all the brokers in a cluster have a similar number of replicas.

#### Anomaly Notifier ####
The anomaly notifier allows users to be notified when an anomaly is detected. Anomalies include:
 * Broker failure
 * Goal violation
 
In addition to anomaly notifications users can specify actions to be taken in response to an anomaly. The following actions are supported:
 * **fix** - fix the problem right away
 * **check** - check the situation again after a given delay
 * **ignore** - ignore the anomaly
