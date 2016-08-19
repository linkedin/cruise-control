Cruise Control for Apache Kafka
===================

### Introduction ###
  Cruise Control is a product that helps run Apache Kafka clusters at large scale. Due to the popularity of 
  Apache Kafka, many companies has bigger and bigger Kafka clusters. At LinkedIn, we have 1800+ Kafka brokers, 
  which means broker dies almost every day and balancing the workload of Kafka also becomes a big overhead. 
  
  Kafka Cruise control is designed to address this operation scalability issue.
  
### Features ###
  Kafka Cruise Control provides the following features out of the box.
  
  * Resource utilization tracking for brokers, topics and partitions.
  
  * Multi-goal rebalance proposal generation
    * Rack-awareness
    * Resource utilization balance (CPU, DISK, Network I/O)
    * Leader traffic distribution
    * Replica distribution for topics
    * Global replica distribution
    * write your own and plug them in
  
  * Anomaly detection and alerting for the Kafka cluster including
    * goal violation
    * broker failure detection
  
  * Admin operations including:
    * Add brokers
    * Decommission brokers
    * Rebalance the cluster

### Quick Start ###
1. Modify config/cruisecontrol.properties to 
    * fill in `bootstrap.servers` and `zookeeper.connect` to the Kafka cluster to be monitored.
    * set `metric.sampler.class` to your implementation (the default sampler class is CruiseControlMetricsReporterSampler) 
    * set `sample.store.class` to your implementation if necessary (the default SampleStore is KafkaSampleStore)
2. Run the following command 
    ```
    ./gradlew jar copyDependantLibs
    ./kafka-cruise-control-start.sh [-jars PATH_TO_YOUR_JAR_1,PATH_TO_YOUR_JAR_2] config/cruisecontrol.properties [port]
    ```
3. visit http://localhost:9090/kafkacruisecontrol/state or http://localhost:\[port\]/kafkacruisecontrol/state if 
you specified the port when starting cruise control. 

### REST API ###
Cruise Control has provided a [REST API](https://github.com/linkedin/cruise-control/wiki/REST-APIs) for users 
to interact with. See the wiki page for more details.

### How Does It Work ###
Cruise Control tries to understand the workload of each replica and provide a optimization 
solution to the current cluster based on this knowledge.

Cruise Control periodically gets the resource utilization samples at both broker and partition level to 
understand the traffic pattern of each partition. Based on the traffic characteristics of all the partitions, 
it derives the load impact of each partition in the brokers. Cruise Control then builds a workload
model to simulate the workload of the Kafka cluster. The goal optimizer will explore different ways to generate 
the cluster workload optimization proposals based on the list of goals specified by the users.

Cruise Control also monitors the liveness of all the brokers in the cluster. When a broker fails in the
cluster, Cruise Control will automatically move the replicas on the failed broker to the healthy brokers to 
avoid the loss of redundancy.

For more details about how Cruise Control achieves that, see 
[these slides](https://www.slideshare.net/JiangjieQin/introduction-to-kafka-cruise-control-68180931).

### Configurations for Cruise Control ###
To read more about the configurations. Check the 
[configurations wiki page](https://github.com/linkedin/cruise-control/wiki/Configurations).

### Pluggable Components ###
More about pluggable components can be found in the 
[pluggable components wiki page](https://github.com/linkedin/cruise-control/wiki/Pluggable-Components).
#### Metric Sampler #### 
The metric sampler is one of the most important pluggables in Cruise Control, it allows user to easily 
deploy Cruise Control to various environment and work with the existing metric system.

Cruise Control provides a jmx reporter (to be added in the future) which can be configured in your Apache
Kafka server. It will produce performance metrics to a kafka metrics topic which can be consumed by Kafka 
Cruise Control.

#### Sample Store ####
The Sample Store is used to store the collected metric samples and training samples to external storage. 
One problem in metric sampling is that we are using some derived data from the raw metrics. And the way we 
derive the data relies on the metadata of the cluster at that point. So when we look at the old metrics, if we 
do not know the metadata at the point the metric was collected the derived data would not be accurate. Sample 
Store help solve this problem by storing the derived data directly to an external storage for later loading.

The default sample store implementation produces the metric samples back to Kafka.

#### Goals ####
The goals in Cruise Control pluggable with different priorities. The default priority is:
 * **RackAwareCapacityGoal** - A goal that ensures all the replicas of each partition are assigned in a rack aware manner. All the broker’s 
 resource utilization are below a given threshold.
 * **PotentialNwOutGoal** - A goal that ensures the potential network output (when all the replicas becomes leaders) on each of the broker do not exceed the broker’s network outbound bandwidth capacity.
 * **ResourceDistributionGoal** - Attempt to make the workload variance among all the brokers are within a certain range. This goal does not do anything if the cluster is in a low utilization mode (when all the resource utilization of each broker is below a configured percentage.)
 * **LeaderBytesInDistributsionGoal** - Attempt to make the leader bytes in rate on each host to be balanced.
 * **TopicReplicaDistributionGoal** - Attempt to make the replicas of the same topic are evenly distributed across the entire cluster.
 * **ReplicaDistributionGoal** - Attempt to make all the brokers in a cluster to have the similar amount of replicas.

#### Anomaly Notifier ####
The anomaly notifier allows user to be notified and handled when an anomaly is detected. The anomaly includes:
 * Broker failure
 * Goal violation
 
In the anomaly notifier users needs to return an action to the anomaly, it could be one of the following:
 * **fix** - fix the problem right away.
 * **check** - check the situation again after a given delay
 * **ignore** - ignore the anomaly
