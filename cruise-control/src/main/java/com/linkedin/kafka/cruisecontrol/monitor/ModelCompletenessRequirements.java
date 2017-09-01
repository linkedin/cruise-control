/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor;

/**
 * This class is created to describe the requirements of the model. In cruise control, the requirement
 * of the monitored performance metrics differs in different scenarios. For example, RackAwareness goal do not really
 * need any load information, but it need to include all the topics. On the other hand, resource distribution
 * would need more load information.
 * <p>
 * This class allows different goals to specify different requirements for the cluster model. Currently the following
 * requirements can be specified:
 * </p>
 * <ul>
 *   <li><b>Number of Snapshot Windows</b> - specifies how many snapshots must be there for each partition in
 *   order for a partition to be considered as valid for the cluster model. The snapshots are counted backwards from
 *   the current snapshot window</li>
 *
 *   <li><b>Minimum Monitored Partitions Percentage</b> - specifies the minimum required percentage of
 *   valid partitions out of the all the partitions in the cluster.</li>
 *
 *   <li><b>Whether Include All Topics</b> - For some goals that do not care about the load information at all (e.g.
 *   Rack Awareness Goal), they can simply include all the topics. Notice include all the topics does not
 *   override the other requirements. So if include all topics is specified and a minimum monitored partitions
 *   percentage is not met, the entire requirement will still be considered not met.</li>
 * </ul>
 *
 */
public class ModelCompletenessRequirements {
  private final int _minRequiredNumSnapshotWindows;
  private final double _minMonitoredPartitionsPercentage;
  private final boolean _includeAllTopics;

  /**
   * Constructor for the requirements.
   * @param minRequiredNumSnapshotWindows the minimum number of required snapshots to generate the model. Tha value
   *                                   must positive.
   * @param minMonitoredPartitionPercentage The minimum required percentage of monitored partitions.
   * @param includeAllTopics whether all the topics should be included to the time window. When set to true, all the
   *                         topics will be included even when there is not enough snapshots. An empty snapshot will
   *                         be used if there is no sample for a partition.
   */
  public ModelCompletenessRequirements(int minRequiredNumSnapshotWindows,
                                       double minMonitoredPartitionPercentage,
                                       boolean includeAllTopics) {
    if (minRequiredNumSnapshotWindows <= 0) {
      throw new IllegalArgumentException("Invalid minRequiredNumSnapshotWindows " + minRequiredNumSnapshotWindows +
                                             ". The minRequiredNumSnapshotWindows must be positive.");
    }
    if (minMonitoredPartitionPercentage < 0 || minMonitoredPartitionPercentage > 1) {
      throw new IllegalArgumentException("Invalid minimumMonitoredPartitionsPercentage "
                                             + minMonitoredPartitionPercentage + ". The value must be between 0 and 1"
                                             + ", both inclusive.");
    }
    _minRequiredNumSnapshotWindows = minRequiredNumSnapshotWindows;
    _includeAllTopics = includeAllTopics;
    _minMonitoredPartitionsPercentage = minMonitoredPartitionPercentage;
  }

  public int minRequiredNumSnapshotWindows() {
    return _minRequiredNumSnapshotWindows;
  }

  public double minMonitoredPartitionsPercentage() {
    return _minMonitoredPartitionsPercentage;
  }

  public boolean includeAllTopics() {
    return _includeAllTopics;
  }

  /**
   * Combine the requirements of this ModelCompletenessRequirements and another one. The result should meet both requirements.
   *
   * @param other the other ModelCompletenessRequirements
   * @return the combined stronger model completeness requirements.
   */
  public ModelCompletenessRequirements stronger(ModelCompletenessRequirements other) {
    if (other == null) {
      return this;
    }
    return new ModelCompletenessRequirements(Math.max(_minRequiredNumSnapshotWindows, other.minRequiredNumSnapshotWindows()),
                                             Math.max(_minMonitoredPartitionsPercentage, other.minMonitoredPartitionsPercentage()),
                                             _includeAllTopics || other.includeAllTopics());
  }

  /**
   * Combine the requirements of this ModelCompletenessRequirements and another one.
   * The result will be the weaker weaker one for each requirement in the two specifications.
   *
   * @param other the other ModelCompletenessRequirements
   * @return the combined weaker model completeness requirements.
   */
  public ModelCompletenessRequirements weaker(ModelCompletenessRequirements other) {
    if (other == null) {
      return this;
    }
    return new ModelCompletenessRequirements(Math.min(_minRequiredNumSnapshotWindows, other.minRequiredNumSnapshotWindows()),
                                             Math.min(_minMonitoredPartitionsPercentage, other.minMonitoredPartitionsPercentage()),
                                             _includeAllTopics && other.includeAllTopics());
  }

  @Override
  public String toString() {
    return String.format("(requiredNumSnapshots=%d, minMonitoredPartitionPercentage=%.3f, includedAllTopics=%s)",
                         _minRequiredNumSnapshotWindows, _minMonitoredPartitionsPercentage, _includeAllTopics);
  }

}
