Cruise Control uses [dropwizzard](http://www.dropwizard.io) metrics to report its own status. 

Currently the following metrics are provided. They are useful to monitor the state of Cruise Control itself. We will add more sensors in the future.

| DESCRIPTIONS                                                  | MBEAN NAME                                                                           |
|---------------------------------------------------------------|--------------------------------------------------------------------------------------|
| Broker failure rate                                           | kafka.cruisecontrol:name=AnomalyDetector.broker-failure-rate                         |
| Goal violation rate                                           | kafka.cruisecontrol:name=AnomalyDetector.goal-violation-rate                         |
| Proposal computation time in ms                               | kafka.cruisecontrol:name=GoalOptimizer.proposal-computation-timer                    |
| Cluster model creation time in ms                             | kafka.cruisecontrol:name=LoadMonitor.cluster-model-creation-timer                    |
| The monitored partition percentage                            | kafka.cruisecontrol:name=LoadMonitor.monitored-partitions-percentage                 |
| The number of partitions that is valid but require imputation | kafka.cruisecontrol:name=LoadMonitor.num-partitions-with-flaw                        |
| The number of snapshot windows that is monitored              | kafka.cruisecontrol:name=LoadMonitor.total-monitored-snapshot-windows                |
| The rate of partition metric sample fetch failures            | kafka.cruisecontrol:name=MetricFetcherManager.partition-samples-fetcher-failure-rate |
| The time taken by each round of partition sample fetch        | kafka.cruisecontrol:name=MetricFetcherManager.partition-samples-fetcher-timer        |
| The rate of training sample fetch failures                    | kafka.cruisecontrol:name=MetricFetcherManager.training-samples-fetcher-failure-rate  |
| The time taken by each training sample fetch                  | kafka.cruisecontrol:name=MetricFetcherManager.training-samples-fetcher-timer         |
