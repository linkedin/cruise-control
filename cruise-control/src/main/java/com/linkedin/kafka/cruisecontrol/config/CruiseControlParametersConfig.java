/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.servlet.parameters.AddBrokerParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.AdminParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.BootstrapParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ClusterLoadParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlStateParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.DemoteBrokerParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.KafkaClusterStateParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.PartitionLoadParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.PauseResumeParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ProposalsParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RebalanceParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RemoveBrokerParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ReviewBoardParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ReviewParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.StopProposalParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.TopicConfigurationParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.TrainParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.UserTasksParameters;


/**
 * A class to keep Cruise Control Parameters Configs and defaults.
 */
public class CruiseControlParametersConfig {
  static final String DEFAULT_STOP_PROPOSAL_PARAMETERS_CLASS = StopProposalParameters.class.getName();
  static final String DEFAULT_BOOTSTRAP_PARAMETERS_CLASS = BootstrapParameters.class.getName();
  static final String DEFAULT_TRAIN_PARAMETERS_CLASS = TrainParameters.class.getName();
  static final String DEFAULT_LOAD_PARAMETERS_CLASS = ClusterLoadParameters.class.getName();
  static final String DEFAULT_PARTITION_LOAD_PARAMETERS_CLASS = PartitionLoadParameters.class.getName();
  static final String DEFAULT_PROPOSALS_PARAMETERS_CLASS = ProposalsParameters.class.getName();
  static final String DEFAULT_STATE_PARAMETERS_CLASS = CruiseControlStateParameters.class.getName();
  static final String DEFAULT_KAFKA_CLUSTER_STATE_PARAMETERS_CLASS = KafkaClusterStateParameters.class.getName();
  static final String DEFAULT_USER_TASKS_PARAMETERS_CLASS = UserTasksParameters.class.getName();
  static final String DEFAULT_REVIEW_BOARD_PARAMETERS_CLASS = ReviewBoardParameters.class.getName();
  static final String DEFAULT_ADD_BROKER_PARAMETERS_CLASS = AddBrokerParameters.class.getName();
  static final String DEFAULT_REMOVE_BROKER_PARAMETERS_CLASS = RemoveBrokerParameters.class.getName();
  static final String DEFAULT_REBALANCE_PARAMETERS_CLASS = RebalanceParameters.class.getName();
  static final String DEFAULT_PAUSE_RESUME_PARAMETERS_CLASS = PauseResumeParameters.class.getName();
  static final String DEFAULT_DEMOTE_BROKER_PARAMETERS_CLASS = DemoteBrokerParameters.class.getName();
  static final String DEFAULT_ADMIN_PARAMETERS_CLASS = AdminParameters.class.getName();
  static final String DEFAULT_REVIEW_PARAMETERS_CLASS = ReviewParameters.class.getName();
  static final String DEFAULT_TOPIC_CONFIGURATION_PARAMETERS_CLASS = TopicConfigurationParameters.class.getName();


  private CruiseControlParametersConfig() {

  }

  /**
   * <code>stop.proposal.parameters.class</code>
   */
  public static final String STOP_PROPOSAL_PARAMETERS_CLASS_CONFIG = "stop.proposal.parameters.class";
  static final String STOP_PROPOSAL_PARAMETERS_CLASS_DOC = "The class for parameters of a stop proposal execution request.";

  /**
   * <code>bootstrap.parameters.class</code>
   */
  public static final String BOOTSTRAP_PARAMETERS_CLASS_CONFIG = "bootstrap.parameters.class";
  static final String BOOTSTRAP_PARAMETERS_CLASS_DOC = "The class for parameters of a bootstrap request.";

  /**
   * <code>train.parameters.class</code>
   */
  public static final String TRAIN_PARAMETERS_CLASS_CONFIG = "train.parameters.class";
  static final String TRAIN_PARAMETERS_CLASS_DOC = "The class for parameters of a train request.";

  /**
   * <code>load.parameters.class</code>
   */
  public static final String LOAD_PARAMETERS_CLASS_CONFIG = "load.parameters.class";
  static final String LOAD_PARAMETERS_CLASS_DOC = "The class for parameters of a load request.";

  /**
   * <code>partition.load.parameters.class</code>
   */
  public static final String PARTITION_LOAD_PARAMETERS_CLASS_CONFIG = "partition.load.parameters.class";
  static final String PARTITION_LOAD_PARAMETERS_CLASS_DOC = "The class for parameters of a partition load request.";

  /**
   * <code>proposals.parameters.class</code>
   */
  public static final String PROPOSALS_PARAMETERS_CLASS_CONFIG = "proposals.parameters.class";
  static final String PROPOSALS_PARAMETERS_CLASS_DOC = "The class for parameters of a proposals request.";

  /**
   * <code>state.parameters.class</code>
   */
  public static final String STATE_PARAMETERS_CLASS_CONFIG = "state.parameters.class";
  static final String STATE_PARAMETERS_CLASS_DOC = "The class for parameters of a state request.";

  /**
   * <code>kafka.cluster.state.parameters.class</code>
   */
  public static final String KAFKA_CLUSTER_STATE_PARAMETERS_CLASS_CONFIG = "kafka.cluster.state.parameters.class";
  static final String KAFKA_CLUSTER_STATE_PARAMETERS_CLASS_DOC = "The class for parameters of a kafka cluster state request.";

  /**
   * <code>user.tasks.parameters.class</code>
   */
  public static final String USER_TASKS_PARAMETERS_CLASS_CONFIG = "user.tasks.parameters.class";
  static final String USER_TASKS_PARAMETERS_CLASS_DOC = "The class for parameters of a user tasks request.";

  /**
   * <code>review.board.parameters.class</code>
   */
  public static final String REVIEW_BOARD_PARAMETERS_CLASS_CONFIG = "review.board.parameters.class";
  static final String REVIEW_BOARD_PARAMETERS_CLASS_DOC = "The class for parameters of a review board request.";

  /**
   * <code>add.broker.parameters.class</code>
   */
  public static final String ADD_BROKER_PARAMETERS_CLASS_CONFIG = "add.broker.parameters.class";
  static final String ADD_BROKER_PARAMETERS_CLASS_DOC = "The class for parameters of a add broker request.";

  /**
   * <code>remove.broker.parameters.class</code>
   */
  public static final String REMOVE_BROKER_PARAMETERS_CLASS_CONFIG = "remove.broker.parameters.class";
  static final String REMOVE_BROKER_PARAMETERS_CLASS_DOC = "The class for parameters of a remove broker request.";

  /**
   * <code>rebalance.parameters.class</code>
   */
  public static final String REBALANCE_PARAMETERS_CLASS_CONFIG = "rebalance.parameters.class";
  static final String REBALANCE_PARAMETERS_CLASS_DOC = "The class for parameters of a rebalance request.";

  /**
   * <code>pause.sampling.parameters.class</code>
   */
  public static final String PAUSE_SAMPLING_PARAMETERS_CLASS_CONFIG = "pause.sampling.parameters.class";
  static final String PAUSE_SAMPLING_PARAMETERS_CLASS_DOC = "The class for parameters of a pause sampling request.";

  /**
   * <code>resume.sampling.parameters.class</code>
   */
  public static final String RESUME_SAMPLING_PARAMETERS_CLASS_CONFIG = "resume.sampling.parameters.class";
  static final String RESUME_SAMPLING_PARAMETERS_CLASS_DOC = "The class for parameters of a resume sampling request.";

  /**
   * <code>demote.broker.parameters.class</code>
   */
  public static final String DEMOTE_BROKER_PARAMETERS_CLASS_CONFIG = "demote.broker.parameters.class";
  static final String DEMOTE_BROKER_PARAMETERS_CLASS_DOC = "The class for parameters of a demote broker request.";

  /**
   * <code>admin.parameters.class</code>
   */
  public static final String ADMIN_PARAMETERS_CLASS_CONFIG = "admin.parameters.class";
  static final String ADMIN_PARAMETERS_CLASS_DOC = "The class for parameters of an admin request.";

  /**
   * <code>review.parameters.class</code>
   */
  public static final String REVIEW_PARAMETERS_CLASS_CONFIG = "review.parameters.class";
  static final String REVIEW_PARAMETERS_CLASS_DOC = "The class for parameters of a review request.";

  /**
   * <code>topic.configuration.parameters.class</code>
   */
  public static final String TOPIC_CONFIGURATION_PARAMETERS_CLASS_CONFIG = "topic.configuration.parameters.class";
  static final String TOPIC_CONFIGURATION_PARAMETERS_CLASS_DOC = "The class for parameters of a topic configuration request.";
}
