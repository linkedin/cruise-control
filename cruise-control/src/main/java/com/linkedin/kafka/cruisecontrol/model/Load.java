/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.exception.ModelInputException;

import com.linkedin.kafka.cruisecontrol.monitor.sampling.Snapshot;
import java.io.IOException;
import java.io.OutputStream;

import java.io.Serializable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * A class for representing load information for each resource. Each Load in a cluster must have the same number of
 * snapshots.
 */
public class Load implements Serializable {
  private static final Comparator<Snapshot> TIME_COMPARATOR = (t1, t2) -> Long.compare(t2.time(), t1.time());
  // Number of snapshots in this load.
  private static int _maxNumSnapshots = -1;
  // Snapshots by their time.
  private final List<Snapshot> _snapshotsByTime;
  private final double[] _accumulatedUtilization;
  private final int _maxNumSnapshotForObject;

  /**
   * Initialize the Load class. The initialization should be done once and only once.
   *
   * @param config The configurations for Kafka Cruise Control
   */
  public static void init(KafkaCruiseControlConfig config) {
    if (_maxNumSnapshots > 0) {
      throw new IllegalStateException("The Load has already been initialized.");
    }
    int numSnapshots = config.getInt(KafkaCruiseControlConfig.NUM_LOAD_SNAPSHOTS_CONFIG);
    if (numSnapshots <= 0) {
      throw new IllegalArgumentException("The number of snapshots is " + numSnapshots + ". It must be greater than 0.");
    }
    _maxNumSnapshots = numSnapshots;
  }

  /**
   * Get snapshots in this load by their snapshot time.
   */
  public List<Snapshot> snapshotsByTime() {
    return _snapshotsByTime;
  }

  /**
   * Get the number of snapshots in the load.
   */
  public int numSnapshots() {
    return _snapshotsByTime.size();
  }

  /**
   * Generate a new Load with the number of snapshots specified in {@link #init}.
   */
  public static Load newLoad() {
    if (_maxNumSnapshots <= 0) {
      throw new IllegalStateException("The LoadFactory hasn't been initialized.");
    }
    return new Load(_maxNumSnapshots);
  }

  /**
   * Check if the load is initialized.
   */
  public static boolean initialized() {
    return _maxNumSnapshots > 0;
  }

  /**
   * Get the number of snapshots setting.
   */
  public static int maxNumSnapshots() {
    return _maxNumSnapshots;
  }

  /**
   * Get a single snapshot value that is representative for the given resource. The current algorithm uses
   * (1) the mean of the recent resource load for inbound network load, outbound network load, and cpu load
   * (2) the latest utilization for disk space usage.
   *
   * @param resource Resource for which the expected utilization will be provided.
   * @return A single representative utilization value on a resource.
   */
  public double expectedUtilizationFor(Resource resource) {
    if (resource.equals(Resource.DISK)) {
      if (_snapshotsByTime.isEmpty()) {
        return 0.0;
      }
      return Math.max(0, _snapshotsByTime.get(0).utilizationFor(resource));
    }

    return Math.max(0, _accumulatedUtilization[resource.id()] / _snapshotsByTime.size());
  }

  /**
   * Package constructor for load with given load properties.
   */
  Load(int maxNumSnapshots) {
    _snapshotsByTime = new ArrayList<>(maxNumSnapshots);
    _maxNumSnapshotForObject = maxNumSnapshots;
    _accumulatedUtilization = new double[Resource.values().length];
    for (Resource resource : Resource.cachedValues()) {
      _accumulatedUtilization[resource.id()] = 0.0;
    }
  }

  /**
   * Overwrite the load for given resource with the given load.
   *
   * @param resource           Resource for which the load will be overwritten.
   * @param loadBySnapshotTime Load for the given resource to overwrite the original load by snapshot time.
   */
  void setLoadFor(Resource resource, Map<Long, Double> loadBySnapshotTime) throws ModelInputException {
    if (loadBySnapshotTime.size() != _snapshotsByTime.size()) {
      throw new ModelInputException("Load to set and load for the resources must have exactly " +
                                        _snapshotsByTime.size() + " entries.");
    }

    double delta = 0.0;
    for (Snapshot snapshot : _snapshotsByTime) {
      long time = snapshot.time();
      if (!loadBySnapshotTime.containsKey(time)) {
        throw new IllegalStateException("The new snapshot time does not match the current snapshot time.");
      }
      double oldUtilization = snapshot.utilizationFor(resource);
      double newUtilization = loadBySnapshotTime.get(time);
      snapshot.setUtilizationFor(resource, newUtilization);
      delta += newUtilization - oldUtilization;
    }

    _accumulatedUtilization[resource.id()] = _accumulatedUtilization[resource.id()] + delta;
  }

  /**
   * Clear the utilization for given resource.
   *
   * @param resource Resource for which the utilization will be cleared.
   */
  void clearLoadFor(Resource resource) {
    for (Snapshot snapshot : _snapshotsByTime) {
      snapshot.clearUtilizationFor(resource);
    }
    _accumulatedUtilization[resource.id()] = 0.0;
  }

  /**
   * Push the latest snapshot.
   *
   * @param snapshot Snapshot containing latest time and state for each resource.
   * @throws ModelInputException
   */
  void pushLatestSnapshot(Snapshot snapshot) throws ModelInputException {
    if (_snapshotsByTime.size() >= _maxNumSnapshotForObject) {
      throw new ModelInputException("Already have " + _snapshotsByTime.size() + " snapshots but see a different " +
                                        "snapshot time" + snapshot.time() + ". Existing snapshot times: " +
                                        Arrays.toString(allSnapshotTimes()));
    }
    if (!_snapshotsByTime.isEmpty() && snapshot.time() >= _snapshotsByTime.get(_snapshotsByTime.size() - 1).time()) {
      throw new ModelInputException("Attempt to push an out of order snapshot with timestamp " + snapshot.time() +
                                        " to a replica. Existing snapshot times: " +
                                        Arrays.toString(allSnapshotTimes()));
    }
    _snapshotsByTime.add(snapshot);
    for (Resource r : Resource.cachedValues()) {
      double utilization = _accumulatedUtilization[r.id()];
      _accumulatedUtilization[r.id()] = utilization + snapshot.utilizationFor(r);
    }
  }

  /**
   * Add the given snapshot to the existing load.
   * Used by cluster/rack/broker when the load in the structure is updated with the push of a new snapshot.
   *
   * @param snapshotToAdd Snapshot to add to the original load.
   */
  void addSnapshot(Snapshot snapshotToAdd) {
    getAndMaybeCreateSnapshot(snapshotToAdd.time()).addSnapshot(snapshotToAdd);
    for (Resource r : Resource.cachedValues()) {
      _accumulatedUtilization[r.id()] = _accumulatedUtilization[r.id()] + snapshotToAdd.utilizationFor(r);
    }
  }

  /**
   * Subtract the given snapshot from the existing load.
   *
   * @param snapshotToSubtract Snapshot to subtract from the original load.
   */
  void subtractSnapshot(Snapshot snapshotToSubtract) {
    snapshotForTime(snapshotToSubtract.time()).subtractSnapshot(snapshotToSubtract);
    for (Resource r : Resource.cachedValues()) {
      _accumulatedUtilization[r.id()] = _accumulatedUtilization[r.id()] - snapshotToSubtract.utilizationFor(r);
    }
  }

  /**
   * Add the given load to this load.
   *
   * @param loadToAdd Load to add to this load.
   */
  void addLoad(Load loadToAdd) {
    for (Snapshot snapshot : loadToAdd.snapshotsByTime()) {
      getAndMaybeCreateSnapshot(snapshot.time()).addSnapshot(snapshot);
    }
    for (Resource r : Resource.cachedValues()) {
      _accumulatedUtilization[r.id()] = this._accumulatedUtilization[r.id()] + loadToAdd.accumulatedUtilization()[r.id()];
    }
  }

  /**
   * Subtract the given load from this load.
   *
   * @param loadToSubtract Load to subtract from this load.
   */
  void subtractLoad(Load loadToSubtract) {
    for (Snapshot snapshot : loadToSubtract.snapshotsByTime()) {
      snapshotForTime(snapshot.time()).subtractSnapshot(snapshot);
    }
    for (Resource r : Resource.cachedValues()) {
      _accumulatedUtilization[r.id()] = this._accumulatedUtilization[r.id()] - loadToSubtract.accumulatedUtilization()[r.id()];
    }
  }

  /**
   * Add the given load for the given resource to this load.
   *
   * @param resource                Resource for which the given load will be added.
   * @param loadToAddBySnapshotTime Load to add to this load for the given resource.
   */
  void addLoadFor(Resource resource, Map<Long, Double> loadToAddBySnapshotTime) {
    double delta = 0.0;
    for (Snapshot snapshot : _snapshotsByTime) {
      double loadToAdd = loadToAddBySnapshotTime.get(snapshot.time());
      snapshot.addUtilizationFor(resource, loadToAdd);
      delta += loadToAdd;
    }
    _accumulatedUtilization[resource.id()] = _accumulatedUtilization[resource.id()] + delta;
  }

  /**
   * Subtract the given load for the given resource from this load.
   *
   * @param resource                     Resource for which the given load will be subtracted.
   * @param loadToSubtractBySnapshotTime Load to subtract from this load for the given resource.
   */
  void subtractLoadFor(Resource resource, Map<Long, Double> loadToSubtractBySnapshotTime) {
    double delta = 0.0;
    for (Snapshot snapshot : _snapshotsByTime) {
      double loadToSubtract = loadToSubtractBySnapshotTime.get(snapshot.time());
      snapshot.subtractUtilizationFor(resource, loadToSubtract);
      delta += loadToSubtract;
    }
    _accumulatedUtilization[resource.id()] = _accumulatedUtilization[resource.id()] - delta;
  }

  /**
   * Clear the content of the circular list for each resource.
   */
  void clearLoad() {
    _snapshotsByTime.clear();
    for (Resource r : Resource.cachedValues()) {
      _accumulatedUtilization[r.id()] = 0.0;
    }
  }

  /**
   * Get the load for the requested resource as a mapping from snapshot time to utilization for the given resource.
   *
   * @param resource Resource for which the load will be provided.
   * @return Load of the requested resource as a mapping from snapshot time to utilization for the given resource.
   */
  Map<Long, Double> loadFor(Resource resource) {
    Map<Long, Double> loadForResource = new HashMap<>();

    for (Snapshot snapshot : _snapshotsByTime) {
      loadForResource.put(snapshot.time(), snapshot.utilizationFor(resource));
    }
    return loadForResource;
  }

  private double[] accumulatedUtilization() {
    return _accumulatedUtilization;
  }

  // A binary search by time. package private for testing.
  Snapshot snapshotForTime(long time) {
    int index = Collections.binarySearch(_snapshotsByTime, new Snapshot(time), TIME_COMPARATOR);
    if (index < 0) {
      return null;
    }
    return _snapshotsByTime.get(index);
  }

  // package private for testing.
  Snapshot getAndMaybeCreateSnapshot(long time) {
    // First do a binary search.
    int index = Collections.binarySearch(_snapshotsByTime, new Snapshot(time), TIME_COMPARATOR);
    if (index >= 0) {
      return _snapshotsByTime.get(index);
    }

    Snapshot snapshot = new Snapshot(time);
    _snapshotsByTime.add(-(index + 1), snapshot);
    return snapshot;
  }

  private long[] allSnapshotTimes() {
    long[] times = new long[_snapshotsByTime.size()];
    for (int i = 0; i < _snapshotsByTime.size(); i++) {
      times[i] = _snapshotsByTime.get(i).time();
    }
    return times;
  }

  /**
   * Output writing string representation of this class to the stream.
   * @param out the output stream.
   */
  public void writeTo(OutputStream out) throws IOException {
    out.write("<Load>".getBytes(StandardCharsets.UTF_8));
    for (Snapshot snapshot : _snapshotsByTime) {
      snapshot.writeTo(out);
    }
    out.write("</Load>%n".getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Get string representation of Load in XML format.
   */
  @Override
  public String toString() {
    StringBuilder load = new StringBuilder().append("<Load>");

    for (Snapshot snapshot : _snapshotsByTime) {
      load.append(snapshot.toString());
    }
    return load.append("</Load>%n").toString();
  }
}
