Support for 2-step verification (aka `peer-review` for `POST` requests) has been added in [#582](https://github.com/linkedin/cruise-control/pull/582), and is available in versions `2.0.38` and `0.1.41` (see [releases](https://github.com/linkedin/cruise-control/releases)).

## Motivation
`POST` requests enable users to perform various admin operations for cluster maintenance in Kafka clusters. However, if users mistype a command, it is possible to start an unintended execution (e.g. remove `broker-X` rather than `broker-Y`).

2-step verification aims to help users verify the command they (or their peers) intend to run by letting them review requests explicitly to approve or discard them, and enable execution of only the approved requests.

## How can I enable 2-step verification for POST requests?
Set `two.step.verification.enabled` config to `true` in your `config/cruisecontrol.properties`.

## What are the `POST` endpoints, whose requests require a 2-step verification?
1. `add_broker`,
2. `remove_broker`,
3. `fix_offline_replicas`
4. `rebalance`,
5. `stop_proposal_execution`
6. `pause_sampling`
7. `resume_sampling`
8. `demote_broker`
9. `admin`

## How does 2-step verification work?

### Submitting a new `POST` request for review
The process of submitting a new request for review does not require extra user-input. Users will send `POST` requests to the servlet as before, but rather than getting (1) a response for the corresponding endpoint or potentially a progress response for async endpoints, users will get (2) a [PurgatoryOrReviewResult](https://github.com/linkedin/cruise-control/blob/migrate_to_kafka_2_4/cruise-control/src/main/java/com/linkedin/kafka/cruisecontrol/servlet/response/PurgatoryOrReviewResult.java) response filtered based on the given request -- i.e. it will return the details of only the request that is being submitted for review. Among other details, this response will show request `ID`, `SUBMITTER_ADDRESS`, `SUBMISSION_TIME_MS`, `STATUS`, and `ENDPOINT_WITH_PARAMS`.

### Approving or discarding `POST` request that are pending review
`REVIEW` endpoint enables users to approve or discard `PENDING_REVIEW` requests. This `POST` endpoint takes the following arguments:

    POST /kafkacruisecontrol/review?json=[true/false]&approve=[id1,id2,...]&discard=[id1,id2,...]&reason=[reason-for-review]

Note that each `id` corresponds to an individual request that is pending review. Supported state transitions are (1) `PENDING_REVIEW` -> {`APPROVED`, `DISCARDED`} and (2) `APPROVED` -> {`DISCARDED`, `SUBMITTED`}. A valid request provides at least one of `approve` or `discard` parameter with one or more valid review ids to review.

### Executing an approved `POST` request
An approved request can be executed by sending a request for the reviewed `endpoint` with the approved `review_id`. For example, if a `rebalance` request was reviewed with `reviewId=42`, then we can execute this request via the following command:

    POST /kafkacruisecontrol/rebalance?review_id=42

The response for the above request will be an `OptimizationResult` or potentially a progress response since this is an async endpoint. Note that specifying `json` parameter is not allowed here, because the all parameters regarding the request are already specified in the original request (and are being reviewed by this request).

### Checking the requests that are `PENDING_REVIEW`, `APPROVED`, `SUBMITTED`, or `DISCARDED`
The following command returns the requests in the `Review Board`:

    GET /kafkacruisecontrol/review_board?json=[true/false]&amp;review_ids=[id1,id2,...]

If `review_ids` is not provided, there won't be any filtering based on review ids in the returned response.

## How do I adjust retention of requests in my Purgatory (a.k.a. Review Board)
You may set the following relevant configs in your `config/cruisecontrol.properties` file:
1. Time-based retention: `two.step.purgatory.retention.time.ms`
2. Quantity-based retention: `two.step.purgatory.max.requests`
