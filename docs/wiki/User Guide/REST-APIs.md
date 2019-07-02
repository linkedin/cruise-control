## A NOTE ON USING UUID/COOKIES
For all the requests, make sure that you interact with endpoints using UUID or cookies, explicitly. CC requests have been redesigned as async calls to avoid blocking. Hence, if you don't use UUID nor cookies, you won't be able to see the server response if it takes longer than a predefined time (default: `10 seconds`). You can retrieve the response within a predefined time(default: `6 hours`) using the UUID returned in initial response; or you can reuse the session to get response if the session has not expired (default: `1 minutes`). If you do not specify cookie or UUID in subsequent requests, such requests will each create a new session and excessive number of ongoing requests will make CC unable to create a new session due to hitting the maximum number of active user task limit. `GET` requests that are sent via a web browser typically use cookies by default; hence, you will preserve the session upon multiple calls to the same endpoint via a web browser.

* Here is a quick recap of how to use UUID with requests using `cURL`:
1. Create a cookie associated with a new request: `curl -vv -X POST -c /tmp/mycookie-jar.txt "http://CRUISE_CONTROL_HOST:2540/kafkacruisecontrol/remove_broker?brokerid=1234&dryrun=false"`
2. Record the User-Task-ID in response: `User-Task-ID: 5ce7c299-53b3-48b6-b72e-6623e25bd9a8`
2. Specifying the User-Task-ID in request that has not completed: `curl -vv -X POST -H "User-Task-ID: 5ce7c299-53b3-48b6-b72e-6623e25bd9a8" "http://CRUISE_CONTROL_HOST:2540/kafkacruisecontrol/remove_broker?brokerid=1234&dryrun=false"`

* Here is a quick recap of how to use cookies with requests using `cURL`:
1. Create a cookie associated with a new request: `curl -X POST -c /tmp/mycookie-jar.txt "http://CRUISE_CONTROL_HOST:2540/kafkacruisecontrol/remove_broker?brokerid=1234&dryrun=false"`
2. Use an existing cookie from the created file for a request that has not completed: `curl -X POST -b /tmp/mycookie-jar.txt "http://CRUISE_CONTROL_HOST:2540/kafkacruisecontrol/remove_broker?brokerid=1234&dryrun=false"`

## GET REQUESTS
The GET requests in Kafka Cruise Control REST API are for read only operations, i.e. the operations that do not have any external impacts. The GET requests include the following operations:
* Query the state of Cruise Control
* Query the current cluster load
* Query partition resource utilization
* Query partition and replica state
* Get optimization proposals
* Bootstrap the load monitor
* Train the Linear Regression Model
* Query the active/completed task result within Cruise Control

### Get the state of Kafka Cruise Control
User can query the state of Kafka Cruise Control at any time by issuing an HTTP GET request.

    GET /kafkacruisecontrol/state?verbose=[true/false]&super_verbose=[true/false]&json=[true/false]&substates=[ANALYZER, MONITOR, EXECUTOR,ANOMALY_DETECTOR]

The returned state contains the following information:
* Monitor State:
  * State: **NOT_STARTED** / **RUNNING** / **SAMPLING** / **PAUSED** / **BOOTSTRAPPING** / **TRAINING** / **LOADING**,
  * Bootstrapping progress (If state is BOOTSTRAPPING)
  * Number of valid monitored windows / Number of total monitored windows
  * Number of valid partitions out of the total number of partitions
  * Percentage of the partitions that are valid
* Executor State:
  * State: **NO_TASK_IN_PROGRESS** /
	   **EXECUTION_STARTED** /
	   **REPLICA_MOVEMENT_IN_PROGRESS** /
	   **LEADER_MOVEMENT_IN_PROGRESS**
  * Total number of replicas to move (if state is REPLICA_MOVEMENT_IN_PROGRESS)
  * Number of replicas finished movement (if state is REPLICA_MOVEMENT_IN_PROGRESS)
* Analyzer State:
  * isProposalReady: Is there a proposal cached
  * ReadyGoals: A list of goals that are ready for running
* Anomaly Detector State:
  * selfHealingEnabled: Anomaly type for which self healing is enabled
  * selfHealingDisabled: Anomaly type for which self healing is disabled
  * recentGoalViolations: Recently detected goal violations
  * recentBrokerFailures: Recently detected broker failures
  * recentMetricAnomalies: Recently detected goal metric anomalies

If verbose is set to true, the details about monitored windows and goals will be displayed.
If super_verbose is set to true, the details about extrapolation made on metric samples will be displayed.
If substates is not set, the full state will be displayed; if it is set to the specific substate(s), only state(s) of interest will be displayed and response will be returned faster.

### Get the cluster load
Once Cruise Control Load Monitor shows it is in the RUNNING state, Users can use the following HTTP GET to get the cluster load:

    GET /kafkacruisecontrol/load?time=[TIMESTAMP]&allow_capacity_estimation=[true/false]&json=[true/false]

If the time field is not provided, it is default to the wall clock time. If the number of workload snapshots before the given timestamp is not sufficient to generate a good load model, an exception will be returned.

TIMESTAMP is in milliseconds since the epoch; what System.currentTimeMillis() returns.  The time zone is the time zone of the Cruise Control server.

If allow_capacity_estimation is set to true, for brokers missing capacity information Cruise Control will make estimations based on other brokers in the cluster; otherwise an IllegalStateException will be thrown and shown in response. By default it is true.

The response contains both load-per-broker and load-per-host information. This is specifically useful when multiple brokers are hosted by the same machine.

NOTE: The load shown is only for the load from the valid partitions. i.e the partitions with enough metric samples. So please always check the Monitor's state(via State endpoint) to decide whether the workload is representative enough.

### Query the partition resource utilization
The following GET request gives the partition load sorted by the utilization of a given resource:

    GET /kafkacruisecontrol/partition_load?resource=[RESOURCE]&start=[START_TIMESTAMP]&end=[END_TIMESTAMP]&json=[true/false]&entries=[MAX_NUMBER_OF_PARTITION_LOAD_ENTRIES_TO_RETURN]&topic=[TOPIC]&partition=[START_PARTITION_INDEX-END_PARTITION_INDEX]&allow_capacity_estimation=[true/false]&min_valid_partition_ratio=[PERCENTAGE]&max_load=[true/false]

The returned result would be a partition list sorted by the utilization of the specified resource in the time range specified by `start` and `end`. The resource can be `CPU`, `NW_IN`, `NW_OUT` and `DISK`. By default the `start` is the earliest monitored time, the `end` is current wall clock time, `resource` is `DISK`, and `entries` is the all partitions in the cluster.  This is in milliseconds since the epoch; what System.currentTimeMillis() returns.  The time zone is the time zone of the Cruise Control server.

By specifying `topic` and `partition` parameter, client can filter returned TopicPartition entries. `topic` value will be treated as a regular expression; `partition` value can be set to a single number(e.g. `partition=15`) or a range(e.g. `partition=0-10`)

The `min_valid_partition_ratio` specifies minimal monitored valid partition percentage needed to calculate the partition load. If this parameter is not set in request, the config value `min.valid.partition.ratio` will be used.

The `max_load` parameter specifies whether report the maximal historical value or average historical value.

### Get partition and replica state
The following GET request gives partition healthiness on the cluster:

    GET /kafkacruisecontrol/kafka_cluster_state?json=[true/false]&verbose=[true/false]

The returned result shows distribution of leader/follower/out-of-sync replica information on each broker of the cluster and leader/follower/in-sync/out-of-sync replica information for online(if `verbose`=true)/offline/under-replicated partitions.

### Get optimization proposals
The following GET request returns the optimization proposals generated based on the workload model of the given timestamp. The workload summary before and after the optimization will also be returned.

    GET /kafkacruisecontrol/proposals?goals=[goal1,goal2...]&verbose=[true/false]&ignore_proposal_cache=[true/false]&data_from=[valid_windows/valid_partitions]&kafka_assigner=[true/false]&json=[true/false]&allow_capacity_estimation=[true/false]&excluded_topics=[TOPIC]&use_ready_default_goals=[true/false]

Kafka cruise control tries to precompute the optimization proposal in the background and caches the best proposal to serve when user queries. If users want to have a fresh proposal without reading it from the proposal cache, set the `ignore_proposal_cache` flag to true. The precomputing always uses available valid partitions to generate the proposals.

By default the proposal will be returned from the cache where all the pre-defined goals are used. Detailed information about the reliability of the proposals will also be returned. If users want to run a different set of goals, they can specify the `goals` argument with the goal names (simple class name).

If `verbose` is turned on, Cruise Control will return all the generated proposals. Otherwise a summary of the proposals will be returned.

Users can specify `data_from` to indicate if they want to run the goals with available `valid_windows` or available `valid_partitions` (default: `valid_windows`).

If `kafka_assigner` is turned on, the proposals will be generated in Kafka Assigner mode. This mode performs optimizations using the goals specific to Kafka Assigner -- i.e. goals with name `KafkaAssigner*`.

Users can specify `excluded_topics` to prevent certain topics' replicas from moving in the generated proposals.

If `use_ready_default_goals` is turned on, Cruise Control will use whatever ready goals(based on available metric data) to calculate the proposals.

### Bootstrap the load monitor (NOT RECOMMENDED)
**(This is not recommended because it may cause the inaccurate partition traffic profiling due to missing metadata. Using the SampleStore is always the preferred way.)**
In order for Kafka Cruise Control to work, the first step is to get the metric samples of partitions on a Kafka cluster. Although users can always wait until all the workload snapshot windows are filled up (e.g. 96 one-hour snapshot window will take 4 days to fill in), Kafka Cruise Control supports a bootstrap function to load old metric examples into the load monitor, so Kafka Cruise Control can begin to work sooner. 

There are three bootstrap modes in Kafka Cruise Control:

* **RANGE** mode: Bootstrap the load monitor by giving a start timestamp and end timestamp.

        GET /kafkacruisecontrol/bootstrap?start=[START_TIMESTAMP]&end=[END_TIMESTAMP]&clearmetrics=[true/false]&json=[true/false]

* **SINCE** mode: bootstrap the load monitor by giving a starting timestamp until it catches up with the wall clock time.

        GET /kafkacruisecontrol/bootstrap?start=[START_TIMESTAMP]&clearmetrics=[true/false]&json=[true/false]

* **RECENT** mode: bootstrap the load monitor with the most recent metric samples until all the load snapshot windows needed by the load monitor are filled in. This is the simplest way to bootstrap the load monitor unless some of the load needs to be excluded. 

        GET /kafkacruisecontrol/bootstrap&clearmetrics=[true/false]

All the bootstrap modes has an option of whether to clear all the existing metric samples in Kafka Cruise Control or not. By default, all the bootstrap operations will clear all the existing metrics. Users can set the parameter clearmetrics=false if they want to preserve the existing metrics.

### Train the linear regression model (Testing in progress)
If use.linear.regression.model is set to true, user have to train the linear regression model before bootstrapping or sampling. The following GET request will start training the linear regression model:

    GET /kafkacruisecontrol/train?start=[START_TIMESTAMP]&end=[END_TIMESTAMP]&json=[true/false]

After the linear regression model training is done (users can check the state of Kafka Cruise Control).

### Get the active/completed task list
The following get request allows user to get a full list of all the active/completed(and not recycled) tasks inside Cruise Control, with their initial request detail(request time/IP address/request URL and parameter) and UUID information. User can then use the returned UUID and URL to fetch the result of the specific request.

    GET /kafkacruisecontrol/user_tasks?json=[true/false]&user_task_ids=[list_of_UUID]

User can use `user_task_ids` make Cruise Control only return requests they are interested. By default all the requests get returned.

## POST Requests
The post requests of Kafka Cruise Control REST API are operations that will have impact on the Kafka cluster. The post operations include:
* Add a list of new brokers to Kafka
* Decommission a list of brokers from the Kafka cluster
* Demoting a list of brokers from the Kafka cluster
* Trigger a workload balance
* Stop the current proposal execution task
* Pause metrics load sampling
* Resume metrics load sampling

Most of the POST actions has a dry-run mode, which only generate the proposals and estimated result but not really execute the proposals. To avoid accidentally triggering of data movement, by default all the POST actions are in dry-run mode. To let Kafka Cruise Control actually move data, users need to explicitly set dryrun=false.

### Add brokers to the Kafka cluster
The following POST request adds the given brokers to the Kafka cluster

    POST /kafkacruisecontrol/add_broker?brokerid=[id1,id2...]&goals=[goal1,goal2...]&dryrun=[true/false]&throttle_added_broker=[true/false]&kafka_assigner=[true/false]&json=[true/false]&allow_capacity_estimation=[true/false]&concurrent_partition_movements_per_broker=[concurrency]&concurrent_leader_movements=[concurrency]&data_from=[valid_windows/valid_partitions]&skip_hard_goal_check=[true/false]&excluded_topics=[TOPICS]&use_ready_default_goals=[true/false]&verbose=[true/false]

When adding new brokers to a Kafka cluster, Cruise Control makes sure that the replicas will only be moved from the existing brokers to the provided new broker, but not moved among existing brokers. 

Users can choose whether to throttle the newly added broker during the partition movement. If the new brokers are throttled, the number of partitions concurrently moving into a broker is gated by the request parameter `concurrent_partition_movements_per_broker` or config value `num.concurrent.partition.movements.per.broker` (if request parameter is not set). Similarly, the number of concurrent partition leadership changes is gated by request parameter `concurrent_leader_movements` or config value `num.concurrent.leader.movements`.

Set `skip_hard_goal_check` to true enforcing a sanity check that all the hard goals are included in the `goals` parameter, otherwise an exception will be thrown.

### Remove a broker from the Kafka cluster
The following POST request removes a broker from the Kafka cluster:

    POST /kafkacruisecontrol/remove_broker?brokerid=[id1,id2...]&goals=[goal1,goal2...]&dryrun=[true/false]&throttle_removed_broker=[true/false]&kafka_assigner=[true/false]&json=[true/false]&allow_capacity_estimation=[true/false]&concurrent_partition_movements_per_broker=[concurrency]&concurrent_leader_movements=[concurrency]&data_from=[valid_windows/valid_partitions]&skip_hard_goal_check=[true/false]&excluded_topics=[TOPICS]&use_ready_default_goals=[true/false]&verbose=[true/false]

Similar to adding brokers to a cluster, removing brokers from a cluster will only move partitions from the brokers to be removed to the other existing brokers. There won't be partition movements among remaining brokers.

Users can choose whether to throttle the removed broker during the partition movement. If the removed brokers are throttled, the number of partitions concurrently moving out of a broker and concurrent partition leadership changes are gated in the same way as add_broker above.

Note if the topics specified in `excluded_topics` has replicas on the removed broker, the replicas will still get moved off the broker.

### Demote a broker from the Kafka cluster
The following POST request moves all the leader replicas away from a list of brokers.

    POST /kafkacruisecontrol/demote_broker?brokerid=[id1, id2...]&dryrun=[true/false]&json=[true/false]&allow_capacity_estimation=[true/false]&concurrent_leader_movements=[concurrency]&verbose=[true/false]&skip_urp_demotion=[true/false]&exclude_follower_demotion=[true/false]

Demoting a broker is consist of tow steps.
  * Make all the replicas on given brokers the least preferred replicas for leadership election
    within their corresponding partitions
  * Trigger a preferred leader election on the partitions to migrate
     the leader replicas off the brokers

Set `skip_urp_demotion` to true will skip the operations on partitions which is currently under replicated; Set `exclude_follower_demotion` will skip operations on the partitions which only have follower replicas on the brokers to be demoted. The purpose of these two parameters is to avoid the URP recovery process blocking demoting broker execution.

### Rebalance a cluster
The following POST request will let Kafka Cruise Control rebalance a Kafka cluster

    POST /kafkacruisecontrol/rebalance?goals=[goal1,goal2...]&dryrun=[true/false]&data_from=[valid_windows/valid_partitions]&kafka_assigner=[true/false]&json=[true/false]&allow_capacity_estimation=[true/false]&concurrent_partition_movements_per_broker=[concurrency]&concurrent_leader_movements=[concurrency]&skip_hard_goal_check=[true/false]&excluded_topics=[TOPICS]&use_ready_default_goals=[true/false]&verbose=[true/false]

Similar to the GET interface for getting proposals, the rebalance can also be based on available valid windows or available valid partitions.

**valid_windows:** rebalance the cluster based on the information in the available valid snapshot windows. A valid snapshot window is a windows whose valid monitored partitions coverage meets the requirements of all the goals. (This is the default behavior)

**valid_partitions:** rebalance the cluster based on all the available valid partitions. All the snapshot windows will be included in this case.

Users can only specify either `valid_windows` or `valid_partitions`, but not both.

**goals:** a list of goals to use for rebalance. When goals is provided, the cached proposals will be ignored.

When rebalancing a cluster, all the brokers in the cluster are eligible to give / receive replicas. All the brokers will be throttled during the partition movement.

By default the rebalance will be in DryRun mode. Please explicitly set dryrun to false to execute the proposals. 

### Stop an ongoing execution
The following POST request will let Kafka Cruise Control stop an ongoing `rebalance`, `add_broker`,  `remove_broker` or `demote_broker` operation:

    POST /kafkacruisecontrol/stop_proposal_execution?json=[true/false]

Note that Cruise Control does not wait for the ongoing batch to finish when it stops execution, i.e. the in-progress batch may still be running after Cruise Control stops the execution.

### Pause metrics load sampling
The following POST request will let Kafka Cruise Control pause an ongoing metrics sampling process:

    POST /kafkacruisecontrol/pause_sampling?json=[true/false]

### Resume metrics load sampling
The following POST request will let Kafka Cruise Control resume a paused metrics sampling process:

    POST /kafkacruisecontrol/resume_sampling?json=[true/false]
