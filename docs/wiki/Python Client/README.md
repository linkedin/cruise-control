# cruise-control client
## Introduction
The `cruise-control-client` directory provides a `python` client for `cruise-control`.  

Within `cruise-control-client/cruisecontrolclient/client`, `cccli.py` is a command-line interface to this `python` client, but the `Endpoint`, `Query`, and `Responder` classes can be used outside of `cccli.py`, to supply a `cruise-control` client outside of a command-line interface.
## Getting Started
`requirements.txt` has been supplied to assist in `python` dependency management.  
### Virtual Environment
In short, `setup.py` can be used with [Python's `venv`](https://docs.python.org/3/library/venv.html) to quickly and easily acquire this client's external dependencies.
```bash
cd cruise-control-client
python3.7 -m venv .
. bin/activate
python setup.py install
```
A `requirements.txt` has also been provided to more-precisely pin version requirements, if necessary.
### CLI Usage
First, change into the `cruise-control-client/cruisecontrolclient/client` directory.

Then, `./cccli.py --socket-address {hostname:port} {endpoint} {options}`
where  
* `hostname:port` is the hostname and port of the `cruise-control` that you'd like to communicate with
* `endpoint` is the `cruise-control` endpoint that you'd like to use
* `options` are `argparse` flags that specify `cruise-control` parameters

### Examples
#### State
```
./cccli.py -a someCruiseControlAddress:9090 state
Starting long-running poll of http://someCruiseControlAddress:9090/kafkacruisecontrol/state?substates=executor&json=true
'ExecutorState':
{'state': 'NO_TASK_IN_PROGRESS'}

'version':
1
```
#### Load
```bash
./cccli.py -a someCruiseControlAddress:9090 load
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
#### Rebalance
```
./cccli.py -a someCruiseControlAddress:9090 rebalance --dry-run
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