/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.servlet.handler.async.AddBrokerRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.ClusterLoadRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.CruiseControlStateRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.DemoteRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.PartitionLoadRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.ProposalsRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.RebalanceRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.RemoveBrokerRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.TopicConfigurationRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.sync.AdminRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.sync.BootstrapRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.sync.KafkaClusterStateRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.sync.PauseRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.sync.ResumeRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.sync.ReviewBoardRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.sync.ReviewRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.sync.StopProposalRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.sync.TrainRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.sync.UserTasksRequest;


/**
 * A class to keep Cruise Control Request Configs and defaults.
 */
public class CruiseControlRequestConfigs {
  static final String DEFAULT_STOP_PROPOSAL_REQUEST_CLASS = StopProposalRequest.class.getName();
  static final String DEFAULT_BOOTSTRAP_REQUEST_CLASS = BootstrapRequest.class.getName();
  static final String DEFAULT_TRAIN_REQUEST_CLASS = TrainRequest.class.getName();
  static final String DEFAULT_LOAD_REQUEST_CLASS = ClusterLoadRequest.class.getName();
  static final String DEFAULT_PARTITION_LOAD_REQUEST_CLASS = PartitionLoadRequest.class.getName();
  static final String DEFAULT_PROPOSALS_REQUEST_CLASS = ProposalsRequest.class.getName();
  static final String DEFAULT_STATE_REQUEST_CLASS = CruiseControlStateRequest.class.getName();
  static final String DEFAULT_KAFKA_CLUSTER_STATE_REQUEST_CLASS = KafkaClusterStateRequest.class.getName();
  static final String DEFAULT_USER_TASKS_REQUEST_CLASS = UserTasksRequest.class.getName();
  static final String DEFAULT_REVIEW_BOARD_REQUEST_CLASS = ReviewBoardRequest.class.getName();
  static final String DEFAULT_ADD_BROKER_REQUEST_CLASS = AddBrokerRequest.class.getName();
  static final String DEFAULT_REMOVE_BROKER_REQUEST_CLASS = RemoveBrokerRequest.class.getName();
  static final String DEFAULT_REBALANCE_REQUEST_CLASS = RebalanceRequest.class.getName();
  static final String DEFAULT_PAUSE_SAMPLING_REQUEST_CLASS = PauseRequest.class.getName();
  static final String DEFAULT_RESUME_SAMPLING_REQUEST_CLASS = ResumeRequest.class.getName();
  static final String DEFAULT_DEMOTE_BROKER_REQUEST_CLASS = DemoteRequest.class.getName();
  static final String DEFAULT_ADMIN_REQUEST_CLASS = AdminRequest.class.getName();
  static final String DEFAULT_REVIEW_REQUEST_CLASS = ReviewRequest.class.getName();
  static final String DEFAULT_TOPIC_CONFIGURATION_REQUEST_CLASS = TopicConfigurationRequest.class.getName();


  private CruiseControlRequestConfigs() {

  }

  /**
   * <code>stop.proposal.request.class</code>
   */
  public static final String STOP_PROPOSAL_REQUEST_CLASS_CONFIG = "stop.proposal.request.class";
  static final String STOP_PROPOSAL_REQUEST_CLASS_DOC = "The class to handle a stop proposal execution request.";

  /**
   * <code>bootstrap.request.class</code>
   */
  public static final String BOOTSTRAP_REQUEST_CLASS_CONFIG = "bootstrap.request.class";
  static final String BOOTSTRAP_REQUEST_CLASS_DOC = "The class to handle a bootstrap request.";

  /**
   * <code>train.request.class</code>
   */
  public static final String TRAIN_REQUEST_CLASS_CONFIG = "train.request.class";
  static final String TRAIN_REQUEST_CLASS_DOC = "The class to handle a train request.";

  /**
   * <code>load.request.class</code>
   */
  public static final String LOAD_REQUEST_CLASS_CONFIG = "load.request.class";
  static final String LOAD_REQUEST_CLASS_DOC = "The class to handle a load request.";

  /**
   * <code>partition.load.request.class</code>
   */
  public static final String PARTITION_LOAD_REQUEST_CLASS_CONFIG = "partition.load.request.class";
  static final String PARTITION_LOAD_REQUEST_CLASS_DOC = "The class to handle a partition load request.";

  /**
   * <code>proposals.request.class</code>
   */
  public static final String PROPOSALS_REQUEST_CLASS_CONFIG = "proposals.request.class";
  static final String PROPOSALS_REQUEST_CLASS_DOC = "The class to handle a proposals request.";

  /**
   * <code>state.request.class</code>
   */
  public static final String STATE_REQUEST_CLASS_CONFIG = "state.request.class";
  static final String STATE_REQUEST_CLASS_DOC = "The class to handle a state request.";

  /**
   * <code>kafka.cluster.state.request.class</code>
   */
  public static final String KAFKA_CLUSTER_STATE_REQUEST_CLASS_CONFIG = "kafka.cluster.state.request.class";
  static final String KAFKA_CLUSTER_STATE_REQUEST_CLASS_DOC = "The class to handle a kafka cluster state request.";

  /**
   * <code>user.tasks.request.class</code>
   */
  public static final String USER_TASKS_REQUEST_CLASS_CONFIG = "user.tasks.request.class";
  static final String USER_TASKS_REQUEST_CLASS_DOC = "The class to handle a user tasks request.";

  /**
   * <code>review.board.request.class</code>
   */
  public static final String REVIEW_BOARD_REQUEST_CLASS_CONFIG = "review.board.request.class";
  static final String REVIEW_BOARD_REQUEST_CLASS_DOC = "The class to handle a review board request.";

  /**
   * <code>add.broker.request.class</code>
   */
  public static final String ADD_BROKER_REQUEST_CLASS_CONFIG = "add.broker.request.class";
  static final String ADD_BROKER_REQUEST_CLASS_DOC = "The class to handle a add broker request.";

  /**
   * <code>remove.broker.request.class</code>
   */
  public static final String REMOVE_BROKER_REQUEST_CLASS_CONFIG = "remove.broker.request.class";
  static final String REMOVE_BROKER_REQUEST_CLASS_DOC = "The class to handle a remove broker request.";

  /**
   * <code>rebalance.request.class</code>
   */
  public static final String REBALANCE_REQUEST_CLASS_CONFIG = "rebalance.request.class";
  static final String REBALANCE_REQUEST_CLASS_DOC = "The class to handle a rebalance request.";

  /**
   * <code>pause.sampling.request.class</code>
   */
  public static final String PAUSE_SAMPLING_REQUEST_CLASS_CONFIG = "pause.sampling.request.class";
  static final String PAUSE_SAMPLING_REQUEST_CLASS_DOC = "The class to handle a pause sampling request.";

  /**
   * <code>resume.sampling.request.class</code>
   */
  public static final String RESUME_SAMPLING_REQUEST_CLASS_CONFIG = "resume.sampling.request.class";
  static final String RESUME_SAMPLING_REQUEST_CLASS_DOC = "The class to handle a resume sampling request.";

  /**
   * <code>demote.broker.request.class</code>
   */
  public static final String DEMOTE_BROKER_REQUEST_CLASS_CONFIG = "demote.broker.request.class";
  static final String DEMOTE_BROKER_REQUEST_CLASS_DOC = "The class to handle a demote broker request.";

  /**
   * <code>admin.request.class</code>
   */
  public static final String ADMIN_REQUEST_CLASS_CONFIG = "admin.request.class";
  static final String ADMIN_REQUEST_CLASS_DOC = "The class to handle an admin request.";

  /**
   * <code>review.request.class</code>
   */
  public static final String REVIEW_REQUEST_CLASS_CONFIG = "review.request.class";
  static final String REVIEW_REQUEST_CLASS_DOC = "The class to handle a review request.";

  /**
   * <code>topic.configuration.request.class</code>
   */
  public static final String TOPIC_CONFIGURATION_REQUEST_CLASS_CONFIG = "topic.configuration.request.class";
  static final String TOPIC_CONFIGURATION_REQUEST_CLASS_DOC = "The class to handle a topic configuration request.";
}
