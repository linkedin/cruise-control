# Examples
Below are some (non-exhaustive) examples of how to use `cccli` to accomplish tasks with `cruise-control`.  
- [`add_broker`](#add_broker)
  - [Add brokers to a cluster](#add-brokers-to-a-cluster)
- [`admin`](#admin)
  - [Dynamically set the per-broker partition movement concurrency](#dynamically-set-the-per-broker-partition-movement-concurrency)
  - [Dynamically set the per-cluster leadership movement concurrency](#dynamically-set-the-per-cluster-leadership-movement-concurrency)
  - [Enable self-healing](#enable-self-healing)
  - [Disable self-healing](#disable-self-healing)
- [`kafka_cluster_state`](#kafka_cluster_state)
  - [Get the `kafka` cluster state](#get-the-kafka-cluster-state)
- [`load`](#load)
  - [Get the cluster load](#get-the-cluster-load)
- [`rebalance`](#rebalance)
  - [Trigger a workload rebalance](#trigger-a-workload-rebalance)
- [`remove_broker`](#remove_broker)
  - [Remove all partitions from one or more brokers](#remove-all-partitions-from-one-or-more-brokers)
- [`state`](#state)
  - [Check the state of an ongoing execution](#check-the-state-of-an-ongoing-execution)
- [`stop`](#stop)
  - [Stop an ongoing proposal execution](#stop-an-ongoing-proposal-execution)
- [`topic_configuration`](#topic_configuration)
  - [Dynamically set the replication factor of one or more topics](#dynamically-set-the-replication-factor-of-one-or-more-topics)
- [`user_tasks`](#user_tasks)
  - [Get all user tasks](#get-all-user-tasks)
## `add_broker`
### Add brokers to a cluster
```bash
cccli -a someCruiseControlAddress:9090 add-broker 1500 --dry-run
```
<details>
<summary>sample response</summary>  

```
Starting long-running poll of http://someCruiseControlAddress:9090/kafkacruisecontrol/add_broker?brokerid=1500&allow_capacity_estimation=False&dryrun=True&json=true
'summary':
{'dataToMoveMB': 156,
 'excludedBrokersForLeadership': [],
 'excludedBrokersForReplicaMove': [],
 'excludedTopics': [],
 'intraBrokerDataToMoveMB': 0,
 'monitoredPartitionsPercentage': 98.13196063041687,
 'numIntraBrokerReplicaMovements': 0,
 'numLeaderMovements': 0,
 'numReplicaMovements': 8,
 'recentWindows': 163}

'goalSummary':
RackAwareGoal: NO-ACTION
                       AVG      STD        MIN        MAX
cpu                  17.67     1.71      15.82      19.75
disk            705,834.50 7,766.46 698,927.44 720,480.56
leaderReplicas    2,997.80   637.82   2,508.00   4,228.00
networkInbound    5,316.30   605.01   4,689.12   6,296.78
networkOutbound   1,935.27   441.51   1,184.77   2,519.02
potentialNwOut    4,478.07   323.65   3,994.05   4,795.87
replicas          6,876.80     7.86   6,869.00   6,889.00
topicReplicas         7.13     0.05       0.00     410.00

ReplicaCapacityGoal: NO-ACTION
                       AVG      STD        MIN        MAX
cpu                  17.67     1.71      15.82      19.75
disk            705,834.50 7,766.46 698,927.44 720,480.56
leaderReplicas    2,997.80   637.82   2,508.00   4,228.00
networkInbound    5,316.30   605.01   4,689.12   6,296.78
networkOutbound   1,935.27   441.51   1,184.77   2,519.02
potentialNwOut    4,478.07   323.65   3,994.05   4,795.87
replicas          6,876.80     7.86   6,869.00   6,889.00
topicReplicas         7.13     0.05       0.00     410.00

DiskCapacityGoal: NO-ACTION
                       AVG      STD        MIN        MAX
cpu                  17.67     1.71      15.82      19.75
disk            705,834.50 7,766.46 698,927.44 720,480.56
leaderReplicas    2,997.80   637.82   2,508.00   4,228.00
networkInbound    5,316.30   605.01   4,689.12   6,296.78
networkOutbound   1,935.27   441.51   1,184.77   2,519.02
potentialNwOut    4,478.07   323.65   3,994.05   4,795.87
replicas          6,876.80     7.86   6,869.00   6,889.00
topicReplicas         7.13     0.05       0.00     410.00

NetworkInboundCapacityGoal: NO-ACTION
                       AVG      STD        MIN        MAX
cpu                  17.67     1.71      15.82      19.75
disk            705,834.50 7,766.46 698,927.44 720,480.56
leaderReplicas    2,997.80   637.82   2,508.00   4,228.00
networkInbound    5,316.30   605.01   4,689.12   6,296.78
networkOutbound   1,935.27   441.51   1,184.77   2,519.02
potentialNwOut    4,478.07   323.65   3,994.05   4,795.87
replicas          6,876.80     7.86   6,869.00   6,889.00
topicReplicas         7.13     0.05       0.00     410.00

NetworkOutboundCapacityGoal: NO-ACTION
                       AVG      STD        MIN        MAX
cpu                  17.67     1.71      15.82      19.75
disk            705,834.50 7,766.46 698,927.44 720,480.56
leaderReplicas    2,997.80   637.82   2,508.00   4,228.00
networkInbound    5,316.30   605.01   4,689.12   6,296.78
networkOutbound   1,935.27   441.51   1,184.77   2,519.02
potentialNwOut    4,478.07   323.65   3,994.05   4,795.87
replicas          6,876.80     7.86   6,869.00   6,889.00
topicReplicas         7.13     0.05       0.00     410.00

CpuCapacityGoal: NO-ACTION
                       AVG      STD        MIN        MAX
cpu                  17.67     1.71      15.82      19.75
disk            705,834.50 7,766.46 698,927.44 720,480.56
leaderReplicas    2,997.80   637.82   2,508.00   4,228.00
networkInbound    5,316.30   605.01   4,689.12   6,296.78
networkOutbound   1,935.27   441.51   1,184.77   2,519.02
potentialNwOut    4,478.07   323.65   3,994.05   4,795.87
replicas          6,876.80     7.86   6,869.00   6,889.00
topicReplicas         7.13     0.05       0.00     410.00

ReplicaDistributionGoal: NO-ACTION
                       AVG      STD        MIN        MAX
cpu                  17.67     1.71      15.82      19.75
disk            705,834.50 7,766.46 698,927.44 720,480.56
leaderReplicas    2,997.80   637.82   2,508.00   4,228.00
networkInbound    5,316.30   605.01   4,689.12   6,296.78
networkOutbound   1,935.27   441.51   1,184.77   2,519.02
potentialNwOut    4,478.07   323.65   3,994.05   4,795.87
replicas          6,876.80     7.86   6,869.00   6,889.00
topicReplicas         7.13     0.05       0.00     410.00

PotentialNwOutGoal: NO-ACTION
                       AVG      STD        MIN        MAX
cpu                  17.67     1.71      15.82      19.75
disk            705,834.50 7,766.46 698,927.44 720,480.56
leaderReplicas    2,997.80   637.82   2,508.00   4,228.00
networkInbound    5,316.30   605.01   4,689.12   6,296.78
networkOutbound   1,935.27   441.51   1,184.77   2,519.02
potentialNwOut    4,478.07   323.65   3,994.05   4,795.87
replicas          6,876.80     7.86   6,869.00   6,889.00
topicReplicas         7.13     0.05       0.00     410.00

DiskUsageDistributionGoal: VIOLATED
                       AVG      STD        MIN        MAX
cpu                  17.67     1.71      15.82      19.75
disk            705,834.50 7,766.46 698,927.44 720,480.56
leaderReplicas    2,997.80   637.82   2,508.00   4,228.00
networkInbound    5,316.30   605.01   4,689.12   6,296.78
networkOutbound   1,935.27   441.51   1,184.77   2,519.02
potentialNwOut    4,478.07   323.65   3,994.05   4,795.87
replicas          6,876.80     7.86   6,869.00   6,889.00
topicReplicas         7.13     0.05       0.00     410.00

NetworkInboundUsageDistributionGoal: VIOLATED
                       AVG      STD        MIN        MAX
cpu                  17.67     1.73      15.82      19.75
disk            705,834.50 7,751.04 698,927.44 720,480.56
leaderReplicas    2,997.80   637.82   2,508.00   4,228.00
networkInbound    5,316.30   402.71   4,691.16   5,862.41
networkOutbound   1,935.27   441.51   1,184.77   2,519.02
potentialNwOut    4,478.07   282.66   3,994.05   4,795.87
replicas          6,876.80     8.63   6,865.00   6,889.00
topicReplicas         7.13     0.05       0.00     410.00

NetworkOutboundUsageDistributionGoal: VIOLATED
                       AVG      STD        MIN        MAX
cpu                  17.67     1.73      15.82      19.75
disk            705,834.50 7,751.04 698,927.44 720,480.56
leaderReplicas    2,997.80   637.82   2,508.00   4,228.00
networkInbound    5,316.30   402.71   4,691.16   5,862.41
networkOutbound   1,935.27   441.51   1,184.77   2,519.02
potentialNwOut    4,478.07   282.66   3,994.05   4,795.87
replicas          6,876.80     8.63   6,865.00   6,889.00
topicReplicas         7.13     0.05       0.00     410.00

CpuUsageDistributionGoal: VIOLATED
                       AVG      STD        MIN        MAX
cpu                  17.67     1.73      15.82      19.75
disk            705,834.50 7,751.04 698,927.44 720,480.56
leaderReplicas    2,997.80   637.82   2,508.00   4,228.00
networkInbound    5,316.30   402.71   4,691.16   5,862.41
networkOutbound   1,935.27   441.51   1,184.77   2,519.02
potentialNwOut    4,478.07   282.66   3,994.05   4,795.87
replicas          6,876.80     8.63   6,865.00   6,889.00
topicReplicas         7.13     0.05       0.00     410.00

TopicReplicaDistributionGoal: NO-ACTION
                       AVG      STD        MIN        MAX
cpu                  17.67     1.73      15.82      19.75
disk            705,834.50 7,751.04 698,927.44 720,480.56
leaderReplicas    2,997.80   637.82   2,508.00   4,228.00
networkInbound    5,316.30   402.71   4,691.16   5,862.41
networkOutbound   1,935.27   441.51   1,184.77   2,519.02
potentialNwOut    4,478.07   282.66   3,994.05   4,795.87
replicas          6,876.80     8.63   6,865.00   6,889.00
topicReplicas         7.13     0.05       0.00     410.00

LeaderReplicaDistributionGoal: VIOLATED
                       AVG      STD        MIN        MAX
cpu                  17.67     1.73      15.82      19.75
disk            705,834.50 7,751.04 698,927.44 720,480.56
leaderReplicas    2,997.80   637.82   2,508.00   4,228.00
networkInbound    5,316.30   402.71   4,691.16   5,862.41
networkOutbound   1,935.27   441.51   1,184.77   2,519.02
potentialNwOut    4,478.07   282.66   3,994.05   4,795.87
replicas          6,876.80     8.63   6,865.00   6,889.00
topicReplicas         7.13     0.05       0.00     410.00

LeaderBytesInDistributionGoal: VIOLATED
                       AVG      STD        MIN        MAX
cpu                  17.67     1.73      15.82      19.75
disk            705,834.50 7,751.04 698,927.44 720,480.56
leaderReplicas    2,997.80   637.82   2,508.00   4,228.00
networkInbound    5,316.30   402.71   4,691.16   5,862.41
networkOutbound   1,935.27   441.51   1,184.77   2,519.02
potentialNwOut    4,478.07   282.66   3,994.05   4,795.87
replicas          6,876.80     8.63   6,865.00   6,889.00
topicReplicas         7.13     0.05       0.00     410.00


'loadAfterOptimization':
       BrokerState  CpuPct     DiskMB  DiskPct  FollowerNwInRate                  Host  LeaderNwInRate  Leaders  NwOutRate  PnwOutRate  Replicas
Broker                                                                                                                                                   
1498         ALIVE   19.75 720,480.56     3.78          2,214.04  some-kafka-host-1498        2,477.12     2960   2,189.74    4,795.87      6889
1499         ALIVE   17.18 698,927.44     3.66          3,211.47  some-kafka-host-1499        2,395.21     2774   2,519.02    4,721.31      6870
1500           NEW   19.66 700,045.56     3.67          2,476.62  some-kafka-host-1500        2,646.87     4228   1,859.69    4,407.35      6877
1501         ALIVE   15.82 706,078.00     3.70          2,783.18  some-kafka-host-1501        2,514.57     2508   1,923.16    3,994.05      6883
1502         ALIVE   15.94 703,688.06     3.69          3,361.03  some-kafka-host-1502        2,501.38     2519   1,184.77    4,471.78      6865

'version':
1
```
</details>

## `admin`
### Dynamically set the per-broker partition movement concurrency
Note that this only has an effect for an *ongoing* proposal execution.
```bash
cccli -a someCruiseControlAddress:9090 admin --concurrency 2
```
<details>
<summary>sample response</summary> 

```
Starting long-running poll of http://someCruiseControlAddress:9090/kafkacruisecontrol/admin?concurrent_partition_movements_per_broker=2&json=true
'ongoingConcurrencyChangeRequest':
'Inter-broker partition movement concurrency is set to 2\n'

'version':
1
```
</details>

### Dynamically set the per-cluster leadership movement concurrency
Note that this only has an effect for an *ongoing* proposal execution.
```bash
cccli -a someCruiseControlAddress:9090 admin --leader-concurrency 2
```
<details>
<summary>sample response</summary> 

```
Starting long-running poll of http://someCruiseControlAddress:9090/kafkacruisecontrol/admin?concurrent_leader_movements=2&json=true
'ongoingConcurrencyChangeRequest':
'Leadership movement concurrency is set to 2\n'

'version':
1
```
</details>

### Enable self-healing
Current valid choices here are one or more of  
`'broker_failure', 'goal_violation', 'metric_anomaly'`
```bash
cccli -a someCruiseControlAddress:9090 admin --enable-self-healing-for broker_failure
```
<details>
<summary>sample response</summary>

```
Starting long-running poll of http://someCruiseControlAddress:9090/kafkacruisecontrol/admin?enable_self_healing_for=broker_failure&json=true
'version':
1

'selfHealingEnabledBefore':
{'BROKER_FAILURE': False}

'selfHealingEnabledAfter':
{'BROKER_FAILURE': True}
```
</details>

### Disable self-healing
Current valid choices here are one or more of  
`'broker_failure', 'goal_violation', 'metric_anomaly'`
```bash
cccli -a someCruiseControlAddress:9090 admin admin --disable-self-healing-for broker_failure
```
<details>
<summary>sample response</summary>

```
Starting long-running poll of http://someCruiseControlAddress:9090 admin/kafkacruisecontrol/admin?disable_self_healing_for=broker_failure&json=true
'version':
1

'selfHealingEnabledBefore':
{'BROKER_FAILURE': True}

'selfHealingEnabledAfter':
{'BROKER_FAILURE': False}
```
</details>

## `kafka_cluster_state`
### Get the `kafka` cluster state
```bash
cccli -a someCruiseControlAddress:9090 kafka-cluster-state
```
<details>
<summary>sample response</summary>  

```
Starting long-running poll of http://someCruiseControlAddress:9090/kafkacruisecontrol/kafka_cluster_state?json=true
'KafkaPartitionState':
    'offline'
[]
    'urp'
[]
    'with-offline-replicas'
[]
    'under-min-isr'
[]

'KafkaBrokerState':
          ReplicaCountByBrokerId  OutOfSyncCountByBrokerId  LeaderCountByBrokerId  OfflineReplicaCountByBrokerId OfflineLogDirsByBrokerId            OnlineLogDirsByBrokerId
BrokerId                                                                                                                                                                    
1498                        7033                         0                   3005                              0                           /export/content/kafka/i001_caches
1499                        7014                         0                   2816                              0                           /export/content/kafka/i001_caches
1500                        7013                         0                   4274                              0                           /export/content/kafka/i001_caches
1501                        7027                         0                   2560                              0                           /export/content/kafka/i001_caches
1502                        7017                         0                   2574                              0                           /export/content/kafka/i001_caches

'version':
1
```
</details>

## `load`
### Get the cluster load
```bash
cccli -a someCruiseControlAddress:9090 load
```
<details>
<summary>sample response</summary>  

```
Starting long-running poll of http://someCruiseControlAddress:9090/kafkacruisecontrol/load?allow_capacity_estimation=False&json=true
'version':
1

'hosts':
                       CpuPct     DiskMB  DiskPct  FollowerNwInRate  LeaderNwInRate  Leaders  NwOutRate  PnwOutRate  Replicas
Host                                                                                                                                 
some-kafka-host-1425   16.08 420,937.78     2.21            346.75          273.61     4578     563.29    1,681.31     18730
some-kafka-host-1427   12.88 433,809.72     2.27            163.00          104.87     9412     677.65    1,026.96     16805
some-kafka-host-1429   19.75 428,663.50     2.25            423.13          295.61     8983     790.29    2,010.03     19177
some-kafka-host-1433   20.22 414,232.50     2.17            102.44          155.51    11495     672.79    1,322.92     17532
some-kafka-host-2187   11.71 629,250.62     2.20            354.76          230.98     6765     714.39    1,825.22     18710
some-kafka-host-2192   12.34 623,767.00     2.18            280.24          187.04    10431     353.13    1,154.15     19190
some-kafka-host-2199   16.93 646,967.81     2.26            252.68          345.10    11215   1,124.97    1,822.09     18799

'brokers':
       BrokerState  CpuPct     DiskMB  DiskPct  FollowerNwInRate                  Host  LeaderNwInRate  Leaders  NwOutRate  PnwOutRate  Replicas
Broker                                                                                                                                                   
1425         ALIVE   16.08 420,937.78     2.21            346.75  some-kafka-host-1425          273.61     4578     563.29    1,681.31     18730
1427         ALIVE   12.88 433,809.72     2.27            163.00  some-kafka-host-1427          104.87     9412     677.65    1,026.96     16805
1429         ALIVE   19.75 428,663.50     2.25            423.13  some-kafka-host-1429          295.61     8983     790.29    2,010.03     19177
1433         ALIVE   20.22 414,232.50     2.17            102.44  some-kafka-host-1433          155.51    11495     672.79    1,322.92     17532
2187         ALIVE   11.71 629,250.62     2.20            354.76  some-kafka-host-2187          230.98     6765     714.39    1,825.22     18710
2192         ALIVE   12.34 623,767.00     2.18            280.24  some-kafka-host-2192          187.04    10431     353.13    1,154.15     19190
2199         ALIVE   16.93 646,967.81     2.26            252.68  some-kafka-host-2199          345.10    11215   1,124.97    1,822.09     18799
```
</details>

## `rebalance`
### Trigger a workload rebalance
```bash
cccli -a someCruiseControlAddress:9090 rebalance --dry-run
```
<details>
<summary>sample response</summary>  

```
Starting long-running poll of http://someCruiseControlAddress:9090/kafkacruisecontrol/rebalance?allow_capacity_estimation=False&dryrun=True&json=true
'version':
1

'progress':
operation: Rebalance
                           completionPercentage                                                                        description  time-in-ms
    step                                                                                                                                          
    WAITING_FOR_CLUSTER_MODEL                100.00  The job requires a cluster model and it is waiting to get the cluster model lock.          65
    AGGREGATING_METRICS                      100.00            Retrieve the metrics of all the partitions from the aggregated metrics.        7813
    GENERATING_CLUSTER_MODEL                   0.00                                      Generating the cluster model for the cluster.        1732
.........

'version':
1

'progress':
operation: Rebalance
                                                 completionPercentage                                                                        description  time-in-ms
    step                                                                                                                                                                
    WAITING_FOR_CLUSTER_MODEL                                      100.00  The job requires a cluster model and it is waiting to get the cluster model lock.          65
    AGGREGATING_METRICS                                            100.00            Retrieve the metrics of all the partitions from the aggregated metrics.        7813
    GENERATING_CLUSTER_MODEL                                         0.00                                      Generating the cluster model for the cluster.        5451
    OPTIMIZING RackAwareGoal                                       100.00                                                      Optimizing goal RackAwareGoal         460
    OPTIMIZING ReplicaCapacityGoal                                 100.00                                                Optimizing goal ReplicaCapacityGoal         319
    OPTIMIZING DiskCapacityGoal                                    100.00                                                   Optimizing goal DiskCapacityGoal         186
    OPTIMIZING NetworkInboundCapacityGoal                          100.00                                         Optimizing goal NetworkInboundCapacityGoal         187
    OPTIMIZING NetworkOutboundCapacityGoal                         100.00                                        Optimizing goal NetworkOutboundCapacityGoal         284
    OPTIMIZING CpuCapacityGoal                                     100.00                                                    Optimizing goal CpuCapacityGoal         183
    OPTIMIZING ReplicaDistributionGoal                             100.00                                            Optimizing goal ReplicaDistributionGoal         672
    OPTIMIZING PotentialNwOutGoal                                  100.00                                                 Optimizing goal PotentialNwOutGoal         188
    OPTIMIZING DiskUsageDistributionGoal                           100.00                                          Optimizing goal DiskUsageDistributionGoal         455
    OPTIMIZING NetworkInboundUsageDistributionGoal                 100.00                                Optimizing goal NetworkInboundUsageDistributionGoal        2219
    OPTIMIZING NetworkOutboundUsageDistributionGoal                100.00                               Optimizing goal NetworkOutboundUsageDistributionGoal         521
    OPTIMIZING CpuUsageDistributionGoal                              0.00                                           Optimizing goal CpuUsageDistributionGoal         712
.........

'summary':
{'dataToMoveMB': 87290,
 'excludedBrokersForLeadership': [],
 'excludedBrokersForReplicaMove': [],
 'excludedTopics': ['__some-topic-we-excluded',
                    '__some-other-topic-we-excluded'],
 'intraBrokerDataToMoveMB': 0,
 'monitoredPartitionsPercentage': 99.24935102462769,
 'numIntraBrokerReplicaMovements': 0,
 'numLeaderMovements': 501,
 'numReplicaMovements': 85,
 'recentWindows': 168}

'goalSummary':
RackAwareGoal: NO-ACTION
                       AVG        STD        MIN        MAX
cpu                   0.16       0.03       0.12       0.20
disk            513,930.64 103,743.40 414,232.50 646,967.81
networkInbound      502.25     165.94     257.96     718.74
networkOutbound     699.50     216.86     353.13   1,124.97
potentialNwOut    1,548.96     350.61   1,026.96   2,010.03
replicas         18,420.43     835.51  16,805.00  19,190.00
topicReplicas         4.87       1.31       0.00     342.00

ReplicaCapacityGoal: NO-ACTION
                       AVG        STD        MIN        MAX
cpu                   0.16       0.03       0.12       0.20
disk            513,930.64 103,743.40 414,232.50 646,967.81
networkInbound      502.25     165.94     257.96     718.74
networkOutbound     699.50     216.86     353.13   1,124.97
potentialNwOut    1,548.96     350.61   1,026.96   2,010.03
replicas         18,420.43     835.51  16,805.00  19,190.00
topicReplicas         4.87       1.31       0.00     342.00

DiskCapacityGoal: NO-ACTION
                       AVG        STD        MIN        MAX
cpu                   0.16       0.03       0.12       0.20
disk            513,930.64 103,743.40 414,232.50 646,967.81
networkInbound      502.25     165.94     257.96     718.74
networkOutbound     699.50     216.86     353.13   1,124.97
potentialNwOut    1,548.96     350.61   1,026.96   2,010.03
replicas         18,420.43     835.51  16,805.00  19,190.00
topicReplicas         4.87       1.31       0.00     342.00

NetworkInboundCapacityGoal: NO-ACTION
                       AVG        STD        MIN        MAX
cpu                   0.16       0.03       0.12       0.20
disk            513,930.64 103,743.40 414,232.50 646,967.81
networkInbound      502.25     165.94     257.96     718.74
networkOutbound     699.50     216.86     353.13   1,124.97
potentialNwOut    1,548.96     350.61   1,026.96   2,010.03
replicas         18,420.43     835.51  16,805.00  19,190.00
topicReplicas         4.87       1.31       0.00     342.00

NetworkOutboundCapacityGoal: NO-ACTION
                       AVG        STD        MIN        MAX
cpu                   0.16       0.03       0.12       0.20
disk            513,930.64 103,743.40 414,232.50 646,967.81
networkInbound      502.25     165.94     257.96     718.74
networkOutbound     699.50     216.86     353.13   1,124.97
potentialNwOut    1,548.96     350.61   1,026.96   2,010.03
replicas         18,420.43     835.51  16,805.00  19,190.00
topicReplicas         4.87       1.31       0.00     342.00

CpuCapacityGoal: NO-ACTION
                       AVG        STD        MIN        MAX
cpu                   0.16       0.03       0.12       0.20
disk            513,930.64 103,743.40 414,232.50 646,967.81
networkInbound      502.25     165.94     257.96     718.74
networkOutbound     699.50     216.86     353.13   1,124.97
potentialNwOut    1,548.96     350.61   1,026.96   2,010.03
replicas         18,420.43     835.51  16,805.00  19,190.00
topicReplicas         4.87       1.31       0.00     342.00

ReplicaDistributionGoal: NO-ACTION
                       AVG        STD        MIN        MAX
cpu                   0.16       0.03       0.12       0.20
disk            513,930.64 103,743.40 414,232.50 646,967.81
networkInbound      502.25     165.94     257.96     718.74
networkOutbound     699.50     216.86     353.13   1,124.97
potentialNwOut    1,548.96     350.61   1,026.96   2,010.03
replicas         18,420.43     835.51  16,805.00  19,190.00
topicReplicas         4.87       1.31       0.00     342.00

PotentialNwOutGoal: NO-ACTION
                       AVG        STD        MIN        MAX
cpu                   0.16       0.03       0.12       0.20
disk            513,930.64 103,743.40 414,232.50 646,967.81
networkInbound      502.25     165.94     257.96     718.74
networkOutbound     699.50     216.86     353.13   1,124.97
potentialNwOut    1,548.96     350.61   1,026.96   2,010.03
replicas         18,420.43     835.51  16,805.00  19,190.00
topicReplicas         4.87       1.31       0.00     342.00

DiskUsageDistributionGoal: FIXED
                       AVG        STD        MIN        MAX
cpu                   0.16       0.03       0.12       0.20
disk            513,930.64 104,371.59 416,369.31 641,207.38
networkInbound      502.25     165.94     257.96     718.74
networkOutbound     699.50     216.86     353.13   1,124.97
potentialNwOut    1,548.96     350.61   1,026.96   2,010.03
replicas         18,420.43     835.70  16,804.00  19,191.00
topicReplicas         4.87       1.31       0.00     342.00

NetworkInboundUsageDistributionGoal: VIOLATED
                       AVG        STD        MIN        MAX
cpu                   0.16       0.04       0.11       0.24
disk            513,930.64 103,557.34 415,586.19 645,130.62
networkInbound      502.25      14.65     480.81     525.05
networkOutbound     699.50     198.98     359.63   1,039.52
potentialNwOut    1,548.96     247.92   1,160.65   1,802.20
replicas         18,420.43     806.58  16,854.00  19,192.00
topicReplicas         4.87       1.31       0.00     342.00

NetworkOutboundUsageDistributionGoal: FIXED
                       AVG        STD        MIN        MAX
cpu                   0.16       0.04       0.11       0.23
disk            513,930.64 103,557.34 415,586.19 645,130.62
networkInbound      502.25      14.65     480.81     525.05
networkOutbound     699.50      22.52     668.19     728.58
potentialNwOut    1,548.96     247.92   1,160.65   1,802.20
replicas         18,420.43     806.58  16,854.00  19,192.00
topicReplicas         4.87       1.31       0.00     342.00

CpuUsageDistributionGoal: VIOLATED
                       AVG        STD        MIN        MAX
cpu                   0.16       0.03       0.12       0.22
disk            513,930.64 103,557.34 415,586.19 645,130.62
networkInbound      502.25      14.65     480.81     525.05
networkOutbound     699.50      24.28     668.02     730.98
potentialNwOut    1,548.96     247.92   1,160.65   1,802.20
replicas         18,420.43     806.58  16,854.00  19,192.00
topicReplicas         4.87       1.31       0.00     342.00

TopicReplicaDistributionGoal: NO-ACTION
                       AVG        STD        MIN        MAX
cpu                   0.16       0.03       0.12       0.22
disk            513,930.64 103,557.34 415,586.19 645,130.62
networkInbound      502.25      14.65     480.81     525.05
networkOutbound     699.50      24.28     668.02     730.98
potentialNwOut    1,548.96     247.92   1,160.65   1,802.20
replicas         18,420.43     806.58  16,854.00  19,192.00
topicReplicas         4.87       1.31       0.00     342.00

LeaderBytesInDistributionGoal: NO-ACTION
                       AVG        STD        MIN        MAX
cpu                   0.16       0.03       0.12       0.22
disk            513,930.64 103,557.34 415,586.19 645,130.62
networkInbound      502.25      14.65     480.81     525.05
networkOutbound     699.50      24.28     668.02     730.98
potentialNwOut    1,548.96     247.92   1,160.65   1,802.20
replicas         18,420.43     806.58  16,854.00  19,192.00
topicReplicas         4.87       1.31       0.00     342.00


'loadAfterOptimization':
       BrokerState  CpuPct     DiskMB  DiskPct  FollowerNwInRate                  Host  LeaderNwInRate  Leaders  NwOutRate  PnwOutRate  Replicas
Broker                                                                                                                                                   
1425         ALIVE   15.89 415,586.19     2.18            289.56  some-kafka-host-1425          235.49     4538     680.48    1,599.18     18707
1427         ALIVE   15.41 430,779.47     2.26            297.88  some-kafka-host-1427          182.93     9271     716.83    1,175.48     16854
1429         ALIVE   15.88 420,803.19     2.21            327.92  some-kafka-host-1429          180.13     8925     679.67    1,802.20     19137
1433         ALIVE   21.80 430,818.34     2.26            273.28  some-kafka-host-1433          208.47    11504     668.02    1,655.12     17580
2187         ALIVE   11.94 628,706.62     2.20            262.86  some-kafka-host-2187          241.72     6796     730.98    1,730.79     18696
2192         ALIVE   15.65 645,130.62     2.25            187.47  some-kafka-host-2192          321.25    10828     689.55    1,160.65     19192
2199         ALIVE   13.33 625,804.56     2.19            284.05  some-kafka-host-2199          222.74    11017     730.98    1,719.27     18777

'version':
1
```
</details>

## `remove_broker`
### Remove all partitions from one or more brokers
```bash
cccli -a someCruiseControlAddress:9090 remove-broker 1500 --dry-run
```
<details>
<summary>sample response</summary>

```
Starting long-running poll of http://someCruiseControlAddress:9090/kafkacruisecontrol/remove_broker?brokerid=1500&allow_capacity_estimation=False&dryrun=True&json=true
'summary':
{'dataToMoveMB': 854804,
 'excludedBrokersForLeadership': [],
 'excludedBrokersForReplicaMove': [],
 'excludedTopics': [''],
 'intraBrokerDataToMoveMB': 0,
 'monitoredPartitionsPercentage': 98.95486235618591,
 'numIntraBrokerReplicaMovements': 0,
 'numLeaderMovements': 20,
 'numReplicaMovements': 7061,
 'recentWindows': 73}

'goalSummary':
RackAwareGoal: FIXED
                         AVG        STD        MIN          MAX
cpu                    21.97       5.46      15.93        29.99
disk            1,051,903.38 223,260.90 855,119.62 1,411,022.25
leaderReplicas      3,827.25   1,311.25       0.00     5,848.00
networkInbound      7,384.69     894.56   6,524.86     8,704.15
networkOutbound     3,055.02   1,165.46   1,631.31     4,301.51
potentialNwOut      7,157.67   1,501.91   5,310.22     9,314.97
replicas            8,836.00   1,812.34       0.00    11,800.00
topicReplicas           8.86       1.96       0.00       681.00

ReplicaCapacityGoal: NO-ACTION
                         AVG        STD        MIN          MAX
cpu                    21.97       5.46      15.93        29.99
disk            1,051,903.38 223,260.90 855,119.62 1,411,022.25
leaderReplicas      3,827.25   1,311.25       0.00     5,848.00
networkInbound      7,384.69     894.56   6,524.86     8,704.15
networkOutbound     3,055.02   1,165.46   1,631.31     4,301.51
potentialNwOut      7,157.67   1,501.91   5,310.22     9,314.97
replicas            8,836.00   1,812.34       0.00    11,800.00
topicReplicas           8.86       1.96       0.00       681.00

DiskCapacityGoal: NO-ACTION
                         AVG        STD        MIN          MAX
cpu                    21.97       5.46      15.93        29.99
disk            1,051,903.38 223,260.90 855,119.62 1,411,022.25
leaderReplicas      3,827.25   1,311.25       0.00     5,848.00
networkInbound      7,384.69     894.56   6,524.86     8,704.15
networkOutbound     3,055.02   1,165.46   1,631.31     4,301.51
potentialNwOut      7,157.67   1,501.91   5,310.22     9,314.97
replicas            8,836.00   1,812.34       0.00    11,800.00
topicReplicas           8.86       1.96       0.00       681.00

NetworkInboundCapacityGoal: NO-ACTION
                         AVG        STD        MIN          MAX
cpu                    21.97       5.46      15.93        29.99
disk            1,051,903.38 223,260.90 855,119.62 1,411,022.25
leaderReplicas      3,827.25   1,311.25       0.00     5,848.00
networkInbound      7,384.69     894.56   6,524.86     8,704.15
networkOutbound     3,055.02   1,165.46   1,631.31     4,301.51
potentialNwOut      7,157.67   1,501.91   5,310.22     9,314.97
replicas            8,836.00   1,812.34       0.00    11,800.00
topicReplicas           8.86       1.96       0.00       681.00

NetworkOutboundCapacityGoal: NO-ACTION
                         AVG        STD        MIN          MAX
cpu                    21.97       5.46      15.93        29.99
disk            1,051,903.38 223,260.90 855,119.62 1,411,022.25
leaderReplicas      3,827.25   1,311.25       0.00     5,848.00
networkInbound      7,384.69     894.56   6,524.86     8,704.15
networkOutbound     3,055.02   1,165.46   1,631.31     4,301.51
potentialNwOut      7,157.67   1,501.91   5,310.22     9,314.97
replicas            8,836.00   1,812.34       0.00    11,800.00
topicReplicas           8.86       1.96       0.00       681.00

CpuCapacityGoal: NO-ACTION
                         AVG        STD        MIN          MAX
cpu                    21.97       5.46      15.93        29.99
disk            1,051,903.38 223,260.90 855,119.62 1,411,022.25
leaderReplicas      3,827.25   1,311.25       0.00     5,848.00
networkInbound      7,384.69     894.56   6,524.86     8,704.15
networkOutbound     3,055.02   1,165.46   1,631.31     4,301.51
potentialNwOut      7,157.67   1,501.91   5,310.22     9,314.97
replicas            8,836.00   1,812.34       0.00    11,800.00
topicReplicas           8.86       1.96       0.00       681.00

ReplicaDistributionGoal: FIXED
                         AVG        STD        MIN          MAX
cpu                    21.97       5.42      16.00        29.92
disk            1,051,903.38 223,260.90 855,119.62 1,411,022.25
leaderReplicas      3,827.25     681.44       0.00     4,550.00
networkInbound      7,384.69     878.14   6,550.69     8,677.07
networkOutbound     3,055.02   1,164.86   1,631.31     4,300.18
potentialNwOut      7,157.67   1,404.83   5,455.31     9,168.33
replicas            8,836.00     620.51       0.00     9,632.00
topicReplicas           8.86       1.46       0.00       681.00

PotentialNwOutGoal: NO-ACTION
                         AVG        STD        MIN          MAX
cpu                    21.97       5.42      16.00        29.92
disk            1,051,903.38 223,260.90 855,119.62 1,411,022.25
leaderReplicas      3,827.25     681.44       0.00     4,550.00
networkInbound      7,384.69     878.14   6,550.69     8,677.07
networkOutbound     3,055.02   1,164.86   1,631.31     4,300.18
potentialNwOut      7,157.67   1,404.83   5,455.31     9,168.33
replicas            8,836.00     620.51       0.00     9,632.00
topicReplicas           8.86       1.46       0.00       681.00

DiskUsageDistributionGoal: FIXED
                         AVG      STD          MIN          MAX
cpu                    21.97     2.38        18.47        24.47
disk            1,051,903.38 8,448.53 1,043,907.38 1,066,113.62
leaderReplicas      3,827.25   636.88         0.00     4,550.00
networkInbound      7,384.69   233.21     7,031.52     7,653.58
networkOutbound     3,055.02   940.46     1,842.26     4,101.97
potentialNwOut      7,157.67   929.80     5,932.00     8,400.55
replicas            8,836.00   540.55         0.00     9,489.00
topicReplicas           8.86     1.37         0.00       549.00

NetworkInboundUsageDistributionGoal: FIXED
                         AVG      STD          MIN          MAX
cpu                    21.97     2.38        18.47        24.47
disk            1,051,903.38 8,448.76 1,043,905.62 1,066,113.62
leaderReplicas      3,827.25   636.88         0.00     4,550.00
networkInbound      7,384.69   196.41     7,087.37     7,597.74
networkOutbound     3,055.02   940.46     1,842.26     4,101.97
potentialNwOut      7,157.67   913.06     5,932.00     8,400.55
replicas            8,836.00   540.10         0.00     9,489.00
topicReplicas           8.86     1.37         0.00       549.00

NetworkOutboundUsageDistributionGoal: FIXED
                         AVG      STD          MIN          MAX
cpu                    21.97     1.18        20.22        23.52
disk            1,051,903.38 8,448.76 1,043,905.62 1,066,113.62
leaderReplicas      3,827.25   630.37         0.00     4,543.00
networkInbound      7,384.69   196.41     7,087.37     7,597.74
networkOutbound     3,055.02    72.40     2,938.04     3,133.23
potentialNwOut      7,157.67   913.06     5,932.00     8,400.55
replicas            8,836.00   540.10         0.00     9,489.00
topicReplicas           8.86     1.37         0.00       549.00

CpuUsageDistributionGoal: FIXED
                         AVG      STD          MIN          MAX
cpu                    21.97     0.69        20.99        22.93
disk            1,051,903.38 8,448.76 1,043,905.62 1,066,113.62
leaderReplicas      3,827.25   619.51         0.00     4,532.00
networkInbound      7,384.69   185.46     7,087.37     7,597.74
networkOutbound     3,055.02    86.73     2,917.54     3,133.23
potentialNwOut      7,157.67   535.57     6,510.71     7,821.84
replicas            8,836.00   540.10         0.00     9,489.00
topicReplicas           8.86     1.38         0.00       549.00

TopicReplicaDistributionGoal: NO-ACTION
                         AVG      STD          MIN          MAX
cpu                    21.97     0.69        20.99        22.93
disk            1,051,903.38 8,448.76 1,043,905.62 1,066,113.62
leaderReplicas      3,827.25   619.51         0.00     4,532.00
networkInbound      7,384.69   185.46     7,087.37     7,597.74
networkOutbound     3,055.02    86.73     2,917.54     3,133.23
potentialNwOut      7,157.67   535.57     6,510.71     7,821.84
replicas            8,836.00   540.10         0.00     9,489.00
topicReplicas           8.86     1.38         0.00       549.00

LeaderReplicaDistributionGoal: VIOLATED
                         AVG       STD          MIN          MAX
cpu                    21.97      0.59        21.32        22.93
disk            1,051,903.38 12,687.47 1,032,969.12 1,066,113.62
leaderReplicas      3,827.25    368.58         0.00     4,355.00
networkInbound      7,384.69    178.52     7,088.03     7,557.75
networkOutbound     3,055.02     97.37     2,917.54     3,158.50
potentialNwOut      7,157.67    519.79     6,543.83     7,821.84
replicas            8,836.00    381.03         0.00     9,489.00
topicReplicas           8.86      1.38         0.00       544.00

LeaderBytesInDistributionGoal: VIOLATED
                         AVG       STD          MIN          MAX
cpu                    21.97      0.59        21.32        22.93
disk            1,051,903.38 12,687.47 1,032,969.12 1,066,113.62
leaderReplicas      3,827.25    368.58         0.00     4,355.00
networkInbound      7,384.69    178.52     7,088.03     7,557.75
networkOutbound     3,055.02     97.37     2,917.54     3,158.50
potentialNwOut      7,157.67    519.79     6,543.83     7,821.84
replicas            8,836.00    381.03         0.00     9,489.00
topicReplicas           8.86      1.38         0.00       544.00


'loadAfterOptimization':
       BrokerState  CpuPct       DiskMB  DiskPct  FollowerNwInRate                    Host  LeaderNwInRate  Leaders  NwOutRate  PnwOutRate  Replicas
Broker                                                                                                                                                     
1498         ALIVE   22.93 1,066,113.62     5.59          3,683.92  some-kafka-broker-1498        3,731.90     4355   2,917.54    7,821.84      9489
1499         ALIVE   21.73 1,032,969.12     5.42          4,464.49  some-kafka-broker-1499        3,093.26     3990   3,009.81    7,494.11      8707
1500          DEAD    0.00         0.00    -1.00              0.00  some-kafka-broker-1500            0.00        0       0.00        0.00         0
1501         ALIVE   21.32 1,060,289.50     5.56          3,615.89  some-kafka-broker-1501        3,861.28     3482   3,158.50    6,543.83      8588
1502         ALIVE   21.90 1,048,280.81     5.50          3,832.07  some-kafka-broker-1502        3,255.96     3482   3,134.23    6,770.89      8560

'version':
1
```
</details>

## `state`
### Check the state of an ongoing execution
```bash
cccli -a someCruiseControlAddress:9090 state
```
<details>
<summary>sample response</summary>

```
Starting long-running poll of http://someCruiseControlAddress:9090/kafkacruisecontrol/state?substates=executor&json=true
'ExecutorState':
{'state': 'NO_TASK_IN_PROGRESS'}

'version':
1
```
</details>

## `stop`
### Stop an ongoing proposal execution
This endpoint:
1. Prevents any *new* partition reassignments from starting, and
2. Cancels (reverts) ongoing partition reassignments.
```bash
cccli -a someCruiseControlAddress:9090 stop
```
<details>
<summary>sample response</summary>

```
Starting long-running poll of http://someCruiseControlAddress:9090/kafkacruisecontrol/stop_proposal_execution?json=true
'message':
'Proposal execution stopped.'

'version':
1

```
</details>

## `topic_configuration`
### Dynamically set the replication factor of one or more topics
Note that `cruise-control` accepts a regular expression to denote which topics are intended.  
[Be careful with this](https://xkcd.com/1171/).
```bash
cccli -a someCruiseControlAddress:9090 topic-configuration --topic some-topic-regex --replication-factor 3
```
<details>
<summary>sample response</summary>

```
Starting long-running poll of http://someCruiseControlAddress:9090/kafkacruisecontrol/topic_configuration?skip_rack_awareness_check=False&replication_factor=3&topic=some-topic-regex&json=true
'summary':
{'dataToMoveMB': 0,
 'excludedBrokersForLeadership': [],
 'excludedBrokersForReplicaMove': [],
 'excludedTopics': [''],
 'intraBrokerDataToMoveMB': 0,
 'monitoredPartitionsPercentage': 98.95486235618591,
 'numIntraBrokerReplicaMovements': 0,
 'numLeaderMovements': 353,
 'numReplicaMovements': 10,
 'recentWindows': 73}

'goalSummary':
RackAwareGoal: NO-ACTION
                       AVG       STD        MIN        MAX
cpu                  17.58      1.66      15.63      20.51
disk            841,522.70 29,585.51 787,726.56 874,522.56
leaderReplicas    3,061.80    635.85   2,576.00   4,290.00
networkInbound    5,907.75    585.13   5,170.29   6,600.34
networkOutbound   2,444.02    667.00   1,630.82   3,467.63
potentialNwOut    5,726.13    687.42   4,616.13   6,547.50
replicas          7,070.80      7.65   7,064.00   7,082.00
topicReplicas         7.09      0.05       0.00     410.00

ReplicaCapacityGoal: NO-ACTION
                       AVG       STD        MIN        MAX
cpu                  17.58      1.66      15.63      20.51
disk            841,522.70 29,585.51 787,726.56 874,522.56
leaderReplicas    3,061.80    635.85   2,576.00   4,290.00
networkInbound    5,907.75    585.13   5,170.29   6,600.34
networkOutbound   2,444.02    667.00   1,630.82   3,467.63
potentialNwOut    5,726.13    687.42   4,616.13   6,547.50
replicas          7,070.80      7.65   7,064.00   7,082.00
topicReplicas         7.09      0.05       0.00     410.00

DiskCapacityGoal: NO-ACTION
                       AVG       STD        MIN        MAX
cpu                  17.58      1.66      15.63      20.51
disk            841,522.70 29,585.51 787,726.56 874,522.56
leaderReplicas    3,061.80    635.85   2,576.00   4,290.00
networkInbound    5,907.75    585.13   5,170.29   6,600.34
networkOutbound   2,444.02    667.00   1,630.82   3,467.63
potentialNwOut    5,726.13    687.42   4,616.13   6,547.50
replicas          7,070.80      7.65   7,064.00   7,082.00
topicReplicas         7.09      0.05       0.00     410.00

NetworkInboundCapacityGoal: NO-ACTION
                       AVG       STD        MIN        MAX
cpu                  17.58      1.66      15.63      20.51
disk            841,522.70 29,585.51 787,726.56 874,522.56
leaderReplicas    3,061.80    635.85   2,576.00   4,290.00
networkInbound    5,907.75    585.13   5,170.29   6,600.34
networkOutbound   2,444.02    667.00   1,630.82   3,467.63
potentialNwOut    5,726.13    687.42   4,616.13   6,547.50
replicas          7,070.80      7.65   7,064.00   7,082.00
topicReplicas         7.09      0.05       0.00     410.00

NetworkOutboundCapacityGoal: NO-ACTION
                       AVG       STD        MIN        MAX
cpu                  17.58      1.66      15.63      20.51
disk            841,522.70 29,585.51 787,726.56 874,522.56
leaderReplicas    3,061.80    635.85   2,576.00   4,290.00
networkInbound    5,907.75    585.13   5,170.29   6,600.34
networkOutbound   2,444.02    667.00   1,630.82   3,467.63
potentialNwOut    5,726.13    687.42   4,616.13   6,547.50
replicas          7,070.80      7.65   7,064.00   7,082.00
topicReplicas         7.09      0.05       0.00     410.00

CpuCapacityGoal: NO-ACTION
                       AVG       STD        MIN        MAX
cpu                  17.58      1.66      15.63      20.51
disk            841,522.70 29,585.51 787,726.56 874,522.56
leaderReplicas    3,061.80    635.85   2,576.00   4,290.00
networkInbound    5,907.75    585.13   5,170.29   6,600.34
networkOutbound   2,444.02    667.00   1,630.82   3,467.63
potentialNwOut    5,726.13    687.42   4,616.13   6,547.50
replicas          7,070.80      7.65   7,064.00   7,082.00
topicReplicas         7.09      0.05       0.00     410.00

ReplicaDistributionGoal: NO-ACTION
                       AVG       STD        MIN        MAX
cpu                  17.58      1.66      15.63      20.51
disk            841,522.70 29,585.51 787,726.56 874,522.56
leaderReplicas    3,061.80    635.85   2,576.00   4,290.00
networkInbound    5,907.75    585.13   5,170.29   6,600.34
networkOutbound   2,444.02    667.00   1,630.82   3,467.63
potentialNwOut    5,726.13    687.42   4,616.13   6,547.50
replicas          7,070.80      7.65   7,064.00   7,082.00
topicReplicas         7.09      0.05       0.00     410.00

PotentialNwOutGoal: NO-ACTION
                       AVG       STD        MIN        MAX
cpu                  17.58      1.66      15.63      20.51
disk            841,522.70 29,585.51 787,726.56 874,522.56
leaderReplicas    3,061.80    635.85   2,576.00   4,290.00
networkInbound    5,907.75    585.13   5,170.29   6,600.34
networkOutbound   2,444.02    667.00   1,630.82   3,467.63
potentialNwOut    5,726.13    687.42   4,616.13   6,547.50
replicas          7,070.80      7.65   7,064.00   7,082.00
topicReplicas         7.09      0.05       0.00     410.00

DiskUsageDistributionGoal: VIOLATED
                       AVG       STD        MIN        MAX
cpu                  17.58      1.66      15.63      20.51
disk            841,522.70 29,585.51 787,726.56 874,522.56
leaderReplicas    3,061.80    635.85   2,576.00   4,290.00
networkInbound    5,907.75    585.13   5,170.29   6,600.34
networkOutbound   2,444.02    667.00   1,630.82   3,467.63
potentialNwOut    5,726.13    687.42   4,616.13   6,547.50
replicas          7,070.80      7.65   7,064.00   7,082.00
topicReplicas         7.09      0.05       0.00     410.00

NetworkInboundUsageDistributionGoal: VIOLATED
                       AVG       STD        MIN        MAX
cpu                  17.58      1.66      15.63      20.51
disk            841,522.70 29,585.51 787,726.56 874,522.56
leaderReplicas    3,061.80    635.85   2,576.00   4,290.00
networkInbound    5,907.75    585.13   5,170.29   6,600.34
networkOutbound   2,444.02    667.00   1,630.82   3,467.63
potentialNwOut    5,726.13    687.42   4,616.13   6,547.50
replicas          7,070.80      7.65   7,064.00   7,082.00
topicReplicas         7.09      0.05       0.00     410.00

NetworkOutboundUsageDistributionGoal: FIXED
                       AVG       STD        MIN        MAX
cpu                  17.58      2.04      15.04      20.96
disk            841,522.70 29,585.51 787,726.56 874,522.56
leaderReplicas    3,061.80    628.33   2,592.00   4,278.00
networkInbound    5,907.75    585.13   5,170.29   6,600.34
networkOutbound   2,444.02     33.59   2,388.38   2,477.59
potentialNwOut    5,726.13    687.42   4,616.13   6,547.50
replicas          7,070.80      7.65   7,064.00   7,082.00
topicReplicas         7.09      0.05       0.00     410.00

CpuUsageDistributionGoal: VIOLATED
                       AVG       STD        MIN        MAX
cpu                  17.58      1.65      15.40      20.31
disk            841,522.70 29,585.51 787,726.56 874,522.56
leaderReplicas    3,061.80    603.04   2,577.00   4,217.00
networkInbound    5,907.75    585.13   5,170.29   6,600.34
networkOutbound   2,444.02     78.36   2,334.03   2,554.00
potentialNwOut    5,726.13    687.42   4,616.13   6,547.50
replicas          7,070.80      7.65   7,064.00   7,082.00
topicReplicas         7.09      0.05       0.00     410.00

TopicReplicaDistributionGoal: NO-ACTION
                       AVG       STD        MIN        MAX
cpu                  17.58      1.65      15.40      20.31
disk            841,522.70 29,585.51 787,726.56 874,522.56
leaderReplicas    3,061.80    603.04   2,577.00   4,217.00
networkInbound    5,907.75    585.13   5,170.29   6,600.34
networkOutbound   2,444.02     78.36   2,334.03   2,554.00
potentialNwOut    5,726.13    687.42   4,616.13   6,547.50
replicas          7,070.80      7.65   7,064.00   7,082.00
topicReplicas         7.09      0.05       0.00     410.00

LeaderReplicaDistributionGoal: VIOLATED
                       AVG       STD        MIN        MAX
cpu                  17.58      1.65      15.40      20.31
disk            841,522.70 29,585.51 787,726.56 874,522.56
leaderReplicas    3,061.80    582.68   2,640.00   4,217.00
networkInbound    5,907.75    585.13   5,170.29   6,600.34
networkOutbound   2,444.02     77.20   2,334.03   2,554.00
potentialNwOut    5,726.13    687.42   4,616.13   6,547.50
replicas          7,070.80      7.65   7,064.00   7,082.00
topicReplicas         7.09      0.05       0.00     410.00

LeaderBytesInDistributionGoal: NO-ACTION
                       AVG       STD        MIN        MAX
cpu                  17.58      1.65      15.40      20.31
disk            841,522.70 29,585.51 787,726.56 874,522.56
leaderReplicas    3,061.80    582.68   2,640.00   4,217.00
networkInbound    5,907.75    585.13   5,170.29   6,600.34
networkOutbound   2,444.02     77.20   2,334.03   2,554.00
potentialNwOut    5,726.13    687.42   4,616.13   6,547.50
replicas          7,070.80      7.65   7,064.00   7,082.00
topicReplicas         7.09      0.05       0.00     410.00


'loadAfterOptimization':
       BrokerState  CpuPct     DiskMB  DiskPct  FollowerNwInRate                    Host  LeaderNwInRate  Leaders  NwOutRate  PnwOutRate  Replicas
Broker                                                                                                                                                   
1498         ALIVE   16.79 835,603.38     4.38          2,399.58  some-kafka-broker-1498        2,873.76     2786   2,498.14    5,422.04      7082
1499         ALIVE   15.40 787,726.56     4.13          3,729.02  some-kafka-broker-1499        2,684.43     2880   2,554.00    6,547.50      7064
1500         ALIVE   20.31 855,413.50     4.49          2,420.27  some-kafka-broker-1500        2,750.02     4217   2,334.03    5,716.02      7064
1501         ALIVE   17.05 854,387.56     4.48          3,186.38  some-kafka-broker-1501        2,894.95     2786   2,392.32    4,616.13      7078
1502         ALIVE   18.34 874,522.56     4.59          3,861.11  some-kafka-broker-1502        2,739.23     2640   2,441.58    6,328.97      7066

'version':
1
```
</details>

## `user_tasks`
### Get all user tasks
```bash
cccli -a someCruiseControlAddress:9090 user-tasks
```
<details>
<summary>sample response</summary>

```
Starting long-running poll of http://someCruiseControlAddress:9090/kafkacruisecontrol/user_tasks?json=true
'userTasks':
[{'ClientIdentity': '2620:119:5002:1003:b958:c11f:96f2:48f7',
  'RequestURL': 'POST /kafkacruisecontrol/stop_proposal_execution?json=true',
  'StartMs': '1564430583214',
  'Status': 'Completed',
  'UserTaskId': '66b3abbb-97d3-4465-bd82-78004a95148c'},
 {'ClientIdentity': '2620:119:5002:1003:b958:c11f:96f2:48f7',
  'RequestURL': 'POST '
                '/kafkacruisecontrol/admin?json=true&concurrent_partition_movements_per_broker=2',
  'StartMs': '1564430085534',
  'Status': 'Completed',
  'UserTaskId': 'd15a2595-7e12-43f7-bf2c-6cad8b0c42c7'},
 {'ClientIdentity': '2620:119:5002:1003:b958:c11f:96f2:48f7',
  'RequestURL': 'POST '
                '/kafkacruisecontrol/admin?json=true&concurrent_leader_movements=2',
  'StartMs': '1564430186239',
  'Status': 'Completed',
  'UserTaskId': '363c62b8-5b68-42cb-9e57-79cb591e94eb'},
 {'ClientIdentity': '2620:119:5002:1003:b958:c11f:96f2:48f7',
  'RequestURL': 'POST '
                '/kafkacruisecontrol/admin?disable_self_healing_for=broker_failure&json=true',
  'StartMs': '1564430353525',
  'Status': 'Completed',
  'UserTaskId': '6f8cba86-bc15-4886-b7f0-0401f6d01192'},
 {'ClientIdentity': '2620:119:5002:1003:b958:c11f:96f2:48f7',
  'RequestURL': 'POST '
                '/kafkacruisecontrol/admin?enable_self_healing_for=broker_failure&json=true',
  'StartMs': '1564430363757',
  'Status': 'Completed',
  'UserTaskId': 'eee8df03-3177-4b24-b0fd-f84f2984eb06'},
 {'ClientIdentity': '2620:119:5002:1003:b958:c11f:96f2:48f7',
  'RequestURL': 'GET /kafkacruisecontrol/kafka_cluster_state?json=true',
  'StartMs': '1564430471875',
  'Status': 'Completed',
  'UserTaskId': '2dac94bd-ff1b-4f0e-b8a2-d30d5fb907a0'},
 {'ClientIdentity': '2620:119:5002:1003:b958:c11f:96f2:48f7',
  'RequestURL': 'GET /kafkacruisecontrol/state?substates=executor&json=true',
  'StartMs': '1564430081119',
  'Status': 'Completed',
  'UserTaskId': '834fc14d-11aa-4482-be28-bef711cbe82c'},
 {'ClientIdentity': '2620:119:5002:1003:b958:c11f:96f2:48f7',
  'RequestURL': 'GET '
                '/kafkacruisecontrol/state?substates=anomaly_detector&json=true',
  'StartMs': '1564430314279',
  'Status': 'Completed',
  'UserTaskId': 'befff953-c3e0-42a8-a43e-8986da6db5db'},
 {'ClientIdentity': '2620:119:5002:1003:b958:c11f:96f2:48f7',
  'RequestURL': 'GET /kafkacruisecontrol/user_tasks?json=true',
  'StartMs': '1564430753365',
  'Status': 'Completed',
  'UserTaskId': '4d0825d3-04f9-4f74-a5e9-683c66014cfd'},
 {'ClientIdentity': '2620:119:5002:1003:b958:c11f:96f2:48f7',
  'RequestURL': 'GET /kafkacruisecontrol/user_tasks?json=true&types=asdfx',
  'StartMs': '1564430778200',
  'Status': 'Completed',
  'UserTaskId': '56b29213-5cef-47ec-bc1c-bc4deda4b6eb'},
 {'ClientIdentity': '2620:119:5002:1003:b958:c11f:96f2:48f7',
  'RequestURL': 'GET '
                '/kafkacruisecontrol/user_tasks?user_task_ids=66b3abbb-97d3-4465-bd82-78004a95148c&json=true',
  'StartMs': '1564430820912',
  'Status': 'Completed',
  'UserTaskId': 'd35602cb-ea1c-4665-9e3c-b3a2b1752067'}]

'version':
1
```
</details>
