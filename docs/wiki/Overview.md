## Architecture
The architecture of Cruise Control is illustrated below:
![Architecture](https://github.com/linkedin/cruise-control/blob/master/docs/images/architecture.png)

### REST API 
Cruise control provides a [REST API](https://github.com/linkedin/cruise-control/wiki/REST-APIs) to allow users to interact with it. The REST API supports querying the workload and optimization proposals of the Kafka cluster, as well as trigger admin operations. 

### Load Monitor 
The Load Monitor collects standard Kafka metrics from the cluster and derives per partition resource metrics that are not directly available (for e.g., it estimates CPU utilization on a per-partition basis). It then generates a cluster workload model that accurately captures cluster resource utilization which includes disk utilization, CPU utilization, bytes-in rate and bytes-out rate at replica-granularity. It then feeds the cluster model into the detector and analyzer.

#### Load Monitor Workflow
![LoadMonitor](https://github.com/linkedin/cruise-control/blob/master/docs/images/loadMonitor.png)
#### Metric Fetcher Manager
The metric fetcher manager is responsible for coordinates all the sampling tasks. The metric fetcher manager creates a configurable number of metric fetcher threads to execute each of sampling tasks. It uses a metric sampler partition assignor to distribute the partitions to different metric fetcher threads.

#### Sampling Tasks
The Load Monitor has a metric fetcher manager to arrange different sampling tasks. There are three types of sampling tasks: 
* Metric Sampling Task
* Bootstrap Task
* Linear Model Training Task
Each sampling tasks is carried out by a configured number of Metric Fetcher threads. Each metric fetcher thread uses a pluggable Metric Sampler to fetch samples. 

#### Metric Sampler Partition Assignor
Each metric fetcher will be assigned a few number of partitions in the cluster to get the samples. The default partition assigner ensures:
* The number of partitions are even for all the metric fetchers
* The partitions of the same topic are assigned to the same metric fetcher

#### Metric Sample Aggregator
Metric Sample Aggregator is a module that organize all the metric samples. The metric sample aggregator will put each metric sample into a Workload Snapshot according to the timestamp of the metric sample.  A workload snapshot is the aggregated metric information during a given time window. For example, if the snapshot time window is 1 hour, and user configured to keep 168 snapshots. Kafka Cruise Control will store 7 days of hourly workload information.

When asked, the metric sampler aggregator will give all the aggregated workload snapshot back to the load monitor. Users can define a minimum number of metric samples required in each workload snapshot to ensure the workload snapshot has enough metric samples. For example, if user specified snapshot window to be 1 hour and the sampling interval is 5 minutes. Ideally each snapshots should have 12 metric samples. User can define that a snapshot window with less than 10 metric samples is invalid and should not be included into the cluster workload model.

#### Cluster Workload Model
The cluster workload model is the primary output of the Load Monitor. It is a software model reflecting the current replica assignment of the cluster with replica granularity workload data for different resources. The cluster model provides interfaces to move partitions or replicas to simulate the impact on the cluster. Those interfaces are used by the Analyzer to generate for the optimization solution. See Build The Workload Model for more details on how the workload is derived.

#### Sample Store
After the metric samples and training samples are accepted by the Metric Sample Aggregator and Cluster Workload Model, they will be passed to a pluggable Sample Store which can be saved to external storage for future loading. This is particularly useful when collecting the old metric samples is difficult or slow.

### Analyzer 
The Analyzer is the "brain" of Cruise Control. It uses a heuristic method to generate optimization proposals based on the user provided optimization goals and the cluster workload model from the Load Monitor. 
Cruise control allows specifying hard goals and soft goals. A hard goal is one that must be satisfied (e.g. replica placement must be rack-aware). Soft goals on the other hand may be left unmet if doing so makes it possible to satisfy all the hard goals. The optimization would fail if the optimized results violate a hard goal. We have implemented the following hard and soft goals so far: 
* Replica placement must be rack-aware 
* Broker resource utilization must be within pre-defined thresholds 
* Network utilization must not be allowed to go beyond a pre-defined capacity even when all replicas on the broker become leaders 
* Attempt to achieve uniform resource utilization across all brokers 
* Attempt to achieve uniform bytes-in rate of leader partitions across brokers 
* Attempt to evenly distribute partitions of a specific topic across all brokers 
* Attempt to evenly distribute replicas (globally) across all brokers 

At a high level, the goal optimization logic is as follows: 
```
For each goal g in the goal list ordered by priority* { 
 For each broker b { 
   while b does not meet g’s requirement { 
     For each replica r on b sorted by the resource utilization density** { 
       Move r (or the leadership of r) to another eligible broker b’ so b’ still satisfies g and all the satisfied goals 
       Finish the optimization for b once g is satisfied. 
     } 
     Fail the optimization if g is a hard goal and is not satisfied for b 
   } 
 } 
 Add g to the satisfied goals 
} 
* The high priority goals are optimized first
**The utilization density is Resource_Utilization / Partition_Size 
```
Note that all the goals are pluggable. Users can write their own goals and add them to Cruise Control via configs.

### Anomaly Detector 
The anomaly detector identifies two types of anomalies: 
* Broker failures – i.e., a non-empty broker leaves a cluster and results in under replicated partitions. Since this can happen during normal cluster bounces as well, the anomaly detector provides for a configurable grace period before it triggers the notifier and fix the cluster. 
* Goal violations - i.e., an optimization goal is violated. If self-healing is enabled, Cruise Control will proactively attempt to address the goal violation by automatically analyzing the workload, and executing optimization proposals. 

### Executor
The executor is responsible for carrying out the optimization proposals from the analyzer. The executor is designed in a way that it is safely interruptible when executing the proposals. The executor ensures that the execution is resource-aware and does not overwhelm any broker.

## Pluggable modules
Cruise Control has a few [pluggable components](https://github.com/linkedin/cruise-control/wiki/Pluggable-Components) to meet various requirements from the users.

## Configurations
Please find all the configurations of Cruise Control in this [wiki page](https://github.com/linkedin/cruise-control/wiki/Configurations)
