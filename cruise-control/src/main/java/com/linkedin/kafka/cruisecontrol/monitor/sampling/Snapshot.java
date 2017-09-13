/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.kafka.cruisecontrol.common.Resource;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;


/**
 * A class for keeping utilization information for each resource and the time of the snapshot.
 */
public class Snapshot implements Serializable {

  private final float[] _utilizationByResource;
  private final long _time;

  /**
   * Create a snapshot with given snapshot time and utilization values for each resource.
   *
   * @param time       Snapshot time in milliseconds.
   * @param cpu        CPU utilization.
   * @param networkIn  Inbound network utilization.
   * @param networkOut Outbound network utilization.
   * @param disk       Disk utilization.
   */
  public Snapshot(long time, double cpu, double networkIn, double networkOut, double disk) {
    _time = time;
    _utilizationByResource = new float[Resource.values().length];
    _utilizationByResource[Resource.CPU.id()] = (float) cpu;
    _utilizationByResource[Resource.NW_IN.id()] = (float) networkIn;
    _utilizationByResource[Resource.NW_OUT.id()] = (float) networkOut;
    _utilizationByResource[Resource.DISK.id()] = (float) disk;
  }

  /**
   * Create a snapshot with 0 resource utilization and given snapshot time. This constructor is used for
   * initialization of snapshot objects in Load class.
   *
   * @param time Snapshot time in milliseconds.
   */
  public Snapshot(long time) {
    this(time, 0.0, 0.0, 0.0, 0.0);
  }

  /**
   * Get utilization for given resource in the snapshot.
   *
   * @param resource Resource for the requested resource utilization in the snapshot.
   * @return Utilization for given resource in the snapshot.
   */
  public double utilizationFor(Resource resource) {
    return _utilizationByResource[resource.id()];
  }

  /**
   * Add utilization from the given resource.
   *
   * @param resource         Resource for which the given utilization value will be added.
   * @param utilizationToAdd Utilization value to be added to the given resource.
   */
  public void addUtilizationFor(Resource resource, double utilizationToAdd) {
    _utilizationByResource[resource.id()] = (float) (utilizationFor(resource) + utilizationToAdd);
  }

  /**
   * Subtract utilization from the given resource.
   *
   * @param resource              Resource for which the given utilization value will be subtracted.
   * @param utilizationToSubtract Utilization value to be subtracted from the given resource.
   */
  public void subtractUtilizationFor(Resource resource, double utilizationToSubtract) {
    _utilizationByResource[resource.id()] = (float) (utilizationFor(resource) - utilizationToSubtract);
  }

  /**
   * Set utilization of the given resource.
   *
   * @param resource    Resource for which the utilization value will be set with the given utilization.
   * @param utilization The value of the new utilization for the given resource.
   */
  public void setUtilizationFor(Resource resource, double utilization) {
    _utilizationByResource[resource.id()] = (float) utilization;
  }

  /**
   * Clear utilization of the given resource.
   *
   * @param resource Resource for which the utilization value will be cleared.
   */
  public void clearUtilizationFor(Resource resource) {
    _utilizationByResource[resource.id()] = 0.0f;
  }

  /**
   * Add utilization of the given snapshot to the utilization of this snapshot.
   *
   * @param snapshotToAdd Snapshot to add to this snapshot.
   */
  public void addSnapshot(Snapshot snapshotToAdd) {
    for (Resource resource : Resource.values()) {
      addUtilizationFor(resource, snapshotToAdd.utilizationFor(resource));
    }
  }

  /**
   * Subtract utilization of the given snapshot from the utilization of this snapshot.
   *
   * @param snapshotToSubtract Snapshot to remove from this snapshot.
   */
  public void subtractSnapshot(Snapshot snapshotToSubtract) {
    for (Resource resource : Resource.values()) {
      subtractUtilizationFor(resource, snapshotToSubtract.utilizationFor(resource));
    }
  }

  /**
   * Get snapshot time. The snapshot represents the aggregated resource utilization of the cluster within
   * a period, the snapshot time is the end time of represented period. e.g. a snapshot with snapshot time T means
   * the snapshot represents the period of [T - SNAPSHOT_INTERVAL, T).
   */
  public long time() {
    return _time;
  }

  /**
   * Get a copy of this Snapshot. The returned snapshot is a new object.
   * @return a copy of this snapshot.
   */
  public Snapshot duplicate() {
    return new Snapshot(_time,
                        _utilizationByResource[Resource.CPU.id()],
                        _utilizationByResource[Resource.NW_IN.id()],
                        _utilizationByResource[Resource.NW_OUT.id()],
                        _utilizationByResource[Resource.DISK.id()]);
  }

  /**
   * Output writing string representation of this class to the stream.
   * @param out the output stream.
   */
  public void writeTo(OutputStream out) throws IOException {
    out.write(String.format("<Snapshot time=\"%d\"", _time).getBytes(StandardCharsets.UTF_8));

    for (Resource r : Resource.values()) {
      out.write(String.format(" %s=\"%.3f\"", r.resource(),
                              _utilizationByResource[r.id()]).getBytes(StandardCharsets.UTF_8));
    }

    out.write(">%n</Snapshot>%n".getBytes(StandardCharsets.UTF_8));
  }

  /*
   * Return an object that can be further used
   * to encode into JSON (version 2 for load stats)
   */
  public Map<String, Object> getJsonStructureForLoad() {
    Map<String, Object> snapshotMap = new HashMap<>();
    snapshotMap.put("time", _time);

    for (Resource r : Resource.values()) {
      snapshotMap.put(r.resource(), _utilizationByResource[r.id()]);
    }

    return snapshotMap;
  }

  /*
   * Return an object that can be further used
   * to encode into JSON
   */
  public Map<String, Object> getJsonStructure() {
    Map<String, Object> snapshotMap = new HashMap<>();
    snapshotMap.put("time", _time);

    List<Object> resourceList = new ArrayList<>();

    for (Resource resource : Resource.values()) {
      Map<String, Object> rMap = new HashMap<>();
      rMap.put("resource", resource);
      rMap.put("utilization", _utilizationByResource[resource.id()]);
      resourceList.add(rMap);
    }

    snapshotMap.put("resources", resourceList);

    return snapshotMap;
  }

  /**
   * Get string representation of Snapshot in XML format.
   */
  @Override
  public String toString() {
    StringBuilder snapshot = new StringBuilder().append(String.format("<Snapshot time=\"%d\"", _time));

    for (Resource resource : Resource.values()) {
      snapshot.append(String.format(" %s=\"%f\"", resource, _utilizationByResource[resource.id()]));
    }

    return snapshot + ">%n</Snapshot>%n";
  }
}
