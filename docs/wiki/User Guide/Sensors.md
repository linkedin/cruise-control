Cruise Control uses [dropwizzard](http://www.dropwizard.io) metrics to report its own status. 

Cruise Control metrics are useful to monitor the state of Cruise Control itself. There are metrics for the following sensor types:

* Executor
* LoadMonitor
* UserTaskManager
* AnomalyDetector
* GoalOptimizer
* MetricFetcherManager
* SessionManager
* KafkaCruiseControlServlet

### Executor Sensors

| DESCRIPTION                                                   | MBEAN NAME                                        						        |
|---------------------------------------------------------------|-----------------------------------------------------------------------------------|
| The number of replica action in progress                      | kafka.cruisecontrol:name=Executor.replica-action-in-progress                      |
| The number of leadership action in progress                   | kafka.cruisecontrol:name=Executor.leadership-action-in-progress                   |
| The number of replica action pending                          | kafka.cruisecontrol:name=Executor.replica-action-pending                          |
| The number of leadership action pending                       | kafka.cruisecontrol:name=Executor.leadership-action-pending                       |
| The number of replica action aborting                         | kafka.cruisecontrol:name=Executor.replica-action-aborting                         |
| The number of leadership action aborting                      | kafka.cruisecontrol:name=Executor.leadership-action-aborting                      |
| The number of replica action aborted                          | kafka.cruisecontrol:name=Executor.replica-action-aborted                          |
| The number of leadership action aborted                       | kafka.cruisecontrol:name=Executor.leadership-action-aborted                       |
| The number of replica action dead                             | kafka.cruisecontrol:name=Executor.replica-action-dead                             |
| The number of leadership action dead                          | kafka.cruisecontrol:name=Executor.leadership-action-dead                          |
| Has an ongoing execution in kafka_assigner mode               | kafka.cruisecontrol:name=Executor.ongoing-execution-kafka_assigner                |
| Has an ongoing execution in non-kafka_assigner mode           | kafka.cruisecontrol:name=Executor.ongoing-execution-non-kafka_assigner            |
| The number of (all) execution stopped                         | kafka.cruisecontrol:name=Executor.execution-stopped                               |
| The number of execution stopped by user                       | kafka.cruisecontrol:name=Executor.execution-stopped-by-user                       |
| The number of execution started in kafka_assigner mode        | kafka.cruisecontrol:name=Executor.execution-started-kafka_assigner                |
| The number of execution started in non-kafka_assigner mode    | kafka.cruisecontrol:name=Executor.execution-started-non-kafka_assigner            |
| Per broker cap on inter-broker partition movements            | kafka.cruisecontrol:name=Executor.inter-broker-partition-movements-per-broker-cap |
| Per broker cap on intra-broker partition movements            | kafka.cruisecontrol:name=Executor.intra-broker-partition-movements-per-broker-cap |
| Global cap on leadership movements                            | kafka.cruisecontrol:name=Executor.leadership-movements-global-cap                 |

### LoadMonitor Sensors

| DESCRIPTION                                                       | MBEAN NAME                                        						|
|-------------------------------------------------------------------|---------------------------------------------------------------------------|
| The number of monitored windows                                   | kafka.cruisecontrol:name=LoadMonitor.total-monitored-windows              |
| The number of partitions that is valid but require extrapolations | kafka.cruisecontrol:name=LoadMonitor.num-partitions-with-extrapolations   |
| The number of valid windows                                       | kafka.cruisecontrol:name=LoadMonitor.valid-windows                        |
| The monitored partition percentage                                | kafka.cruisecontrol:name=LoadMonitor.monitored-partitions-percentage      |
| Cluster model creation time in ms                                 | kafka.cruisecontrol:name=LoadMonitor.cluster-model-creation-timer         |

### UserTaskManager Sensors

| DESCRIPTION                       | MBEAN NAME                                        				|
|-----------------------------------|-------------------------------------------------------------------|
| The number of active sessions     | kafka.cruisecontrol:name=UserTaskManager.num-active-sessions      |
| The number of active user tasks   | kafka.cruisecontrol:name=UserTaskManager.num-active-user-tasks    |

### AnomalyDetector Sensors

| DESCRIPTION                                                                                               | MBEAN NAME                                        				            |
|-----------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------|
| The number of self healing started                                                                        | kafka.cruisecontrol:name=AnomalyDetector.number-of-self-healing-started       |
| The ongoing anomaly duration in ms                                                                        | kafka.cruisecontrol:name=AnomalyDetector.ongoing-anomaly-duration-ms          |
| Whether broker failure self-healing is enabled or not                                                     | kafka.cruisecontrol:name=AnomalyDetector.broker_failure-self-healing-enabled  |
| Whether goal violation self-healing is enabled or not                                                     | kafka.cruisecontrol:name=AnomalyDetector.goal_violation-self-healing-enabled  |
| Whether disk failure self-healing is enabled or not                                                       | kafka.cruisecontrol:name=AnomalyDetector.disk_failure-self-healing-enabled    |
| Whether metric anomaly self-healing is enabled or not                                                     | kafka.cruisecontrol:name=AnomalyDetector.metric_anomaly-self-healing-enabled  |
| Whether topic anomaly self-healing is enabled or not                                                      | kafka.cruisecontrol:name=AnomalyDetector.topic_anomaly-self-healing-enabled   |
| Whether goal violation detector identified goals that require human intervention (e.g. cluster expansion) | kafka.cruisecontrol:name=AnomalyDetector.GOAL_VIOLATION-has-unfixable-goals   |
| Balancedness score (100 = fully-balanced, 0 = fully-unbalanced, -1 = has dead-brokers / disks in cluster) | kafka.cruisecontrol:name=AnomalyDetector.balancedness-score                   |
| Mean time to start a self-healing fix                                                                     | kafka.cruisecontrol:name=AnomalyDetector.mean-time-to-start-fix-ms            |
| Broker failure rate                                                                                       | kafka.cruisecontrol:name=AnomalyDetector.broker-failure-rate                  |
| Goal violation rate                                                                                       | kafka.cruisecontrol:name=AnomalyDetector.goal-violation-rate                  |
| Metric anomaly rate                                                                                       | kafka.cruisecontrol:name=AnomalyDetector.metric-anomaly-rate                  |
| Disk failure rate                                                                                         | kafka.cruisecontrol:name=AnomalyDetector.disk-failure-rate                    |
| Topic anomaly rate                                                                                        | kafka.cruisecontrol:name=AnomalyDetector.topic-anomaly-rate                   |

### GoalOptimizer Sensors

| DESCRIPTION                       | MBEAN NAME                                                        |
|-----------------------------------|-------------------------------------------------------------------|
| Proposal computation time in ms   | kafka.cruisecontrol:name=GoalOptimizer.proposal-computation-timer |


### MetricFetcherManager Sensors

| DESCRIPTION                                               | MBEAN NAME                                                                           |
|-----------------------------------------------------------|--------------------------------------------------------------------------------------|
| The rate of partition metric sample fetch failures        | kafka.cruisecontrol:name=MetricFetcherManager.partition-samples-fetcher-failure-rate |
| The time taken by each round of partition sample fetch    | kafka.cruisecontrol:name=MetricFetcherManager.partition-samples-fetcher-timer        |
| The rate of training sample fetch failures                | kafka.cruisecontrol:name=MetricFetcherManager.training-samples-fetcher-failure-rate  |
| The time taken by each training sample fetch              | kafka.cruisecontrol:name=MetricFetcherManager.training-samples-fetcher-timer         |

### SessionManager Sensors

| DESCRIPTION                   | MBEAN NAME                                                            |
|-------------------------------|-----------------------------------------------------------------------|
| Session lifetime              | kafka.cruisecontrol:name=SessionManager.session-lifetime-timer        |
| Session creation failure rate | kafka.cruisecontrol:name=SessionManager.session-creation-failure-rate |

### KafkaCruiseControlServlet Sensors

| DESCRIPTION                                                   | MBEAN NAME                                                                                            |
|---------------------------------------------------------------|-------------------------------------------------------------------------------------------------------|
| Service time of a successful request in ms for each endpoint  | kafka.cruisecontrol:name=KafkaCruiseControlServlet.<endpoint-name>-successful-request-execution-timer |
| Request rate for each endpoint                                | kafka.cruisecontrol:name=KafkaCruiseControlServlet.<endpoint-name>-request-rate                       |
