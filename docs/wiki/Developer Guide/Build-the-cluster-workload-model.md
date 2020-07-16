## Kafka Raw Metrics
Cruise Control uses utilization of four resources to build its workload model.

| Resource         | Granularity of Raw Metric                     |
|------------------|-----------------------------------------------|
| CPU              | Per broker                                    |
| Disk Utilization | Per replica                                   |
| Bytes In Rate    | All the leader replicas of a topic per broker |
| Bytes Out Rate   | All the leader replicas of a topic per broker |

Because Cruise Control needs the metric sample granularity to be at replica level to build a cluster model, we derive the raw metric of CPU and network to get the replica level metrics.

## Deriving Network IO Metric
The algorithm we use are the following:

Replica_Bytes_In_Rate = 
Topic_Bytes_In_Rate_On_Broker / Num_Partition_Of_The_Topic_On_Broker

Replica_Bytes_Out_Rate = 
Topic_Bytes_In_Rate_On_Broker / Num_Partition_Of_The_Topic_On_Broker

## Deriving CPU Utilization Metric
CPU utilization is more complicated to derive. We are using a bytes-based load model with coefficients. Cruise Control supports two ways to derive the CPU utilization.
* Heuristic Static Model
* Linear Regression Model

### Heuristic Static Model (Current Default Model)
Unlike the [linear regression model](https://github.com/linkedin/cruise-control/wiki/Build-the-cluster-workload-model#linear-regression-model), heuristic static model does not require any training. It uses predefined weights for leader bytes in rate, leader bytes out rate and follower bytes in rate, respectively.

a - CPU contribution weight for Leader_Bytes_In_Rate

b - CPU contribution weight for Leader_Bytes_Out_Rate

c - CPU contribution weight for Follower_Bytes_In_Rate

Partition_Leader_CPU_Utilization = 
Broker_CPU_Utilization * 
(a * Partition_Leader_Bytes_In_Rate + b * Partition_Leader_Bytes_Out_Rate) / 
(a * Leader_Bytes_In_Rate_On_Broker + b * Leader_Bytes_Out_rate_On_Broker + c * Follower_Bytes_In_Rate_On_Broker)

Partition_Follower_CPU_Utilization =
Broker_CPU_Utilization * 
(c * Partition_Follower_Bytes_In_Rate) / 
(a * Leader_Bytes_In_Rate_On_Broker + b * Leader_Bytes_Out_rate_On_Broker + c * Follower_Bytes_In_Rate_On_Broker)

### Linear Regression Model (In development and testing)
The linear regression model requires user to train the model by triggering the training before bootstrapping or filling the samples into the load windows. It demands the training samples to be diverse enough in order to get accurate coefficients.

Broker_CPU_Utilization = 
**a** * Leader_Bytes_In_Rate_On_Broker + **b** * Leader_Bytes_Out_rate_On_Broker + **c** * Follower_Bytes_In_Rate_On_Broker

Where **a**, **b** and **c** are the coefficients that we will build based on historical data. Notice that **a**, **b** and **c** are heavily depending on the following configurations of the broker:
* Compression Codec
* Whether SSL is enabled or not.
So the three coefficient will change based on the configuration.

Once we get the value of **a**, **b** and **c**, the replica cpu utilization can be simply calculated as below:

Leader_Replica_Load = **a** * Replica_Bytes_In_Rate + **b** * Replica_Bytes_Out_Rate

Follower_Replica_Load = **c** * Replica_Bytes_In_Rate

The CPU utilization may be affected by things other than bytes in and out rate such as topic creation / deletion, GC, etc. But typically those operations are taking much less resources and not putting continuous pressure on CPU. The impact of such tasks will be amortized to almost negligible if we look at a long enough period.

Log compaction is another contribution to CPU Utilization, but given that it is also pretty much based on the total bytes produced, by looking at the bytes in rate we have already factored that in.

#### Issues and Solutions to Linear Regression Model:
##### More variables are needed
So far we assumed the CPU utilization is associated only with throughput. This is likely over-simplified. We should collect more metrics to take ProduceRequestRate, ConsumerRequestRate and average request size into consideration. That also means we need to collect per partition request rate as well.

##### Unbalanced Distribution of Metric Samples
Most of the metrics samples collected fall in a narrow range of CPU utilization (e.g. 40% - 60%), a naive regression will cause the model to be biased due to the heavyweight of the samples in this CPU utilization range, thus cause inaccurate estimation when the CPU utilization falls out of this range.

The solution to this issue is to selectively choose the samples to remove the bias introduced by the uneven sample distribution.

##### Correlated Metric Observations
In practice, a Kafka cluster may have a pretty stable BytesIn to BytesOut rate and that may cause the parameter observation matrix to become close to singular and dramatically impact the accuracy of the Linear Regression Model. 

The solution to this is to again select the samples to ensure enough representatives of different BytesIn to BytesOut ratio in the samples.
Inaccurate Micro Estimation
The linear regression model was derived from the broker level metrics. When we want to apply this model to each single partition, it might not be that accurate anymore.

The solution is only apply the model to the broker, i.e. estimate the CPU utilization of the broker after subtract the IO load of the partition, as opposed to computing the CPU utilization of partition first and subtract it from the broker CPU utilization.