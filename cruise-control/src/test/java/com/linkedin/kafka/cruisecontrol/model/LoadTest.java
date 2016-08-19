/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.kafka.cruisecontrol.exception.ModelInputException;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.Snapshot;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class LoadTest {

  @Test
  public void testSnapshotForTime() throws ModelInputException {
    Load load = new Load(3);
    Snapshot s1 = new Snapshot(1L);
    Snapshot s2 = new Snapshot(3L);
    Snapshot s3 = new Snapshot(5L);

    load.pushLatestSnapshot(s1);
    load.pushLatestSnapshot(s2);
    load.pushLatestSnapshot(s3);

    assertTrue(load.snapshotForTime(1L) == s1);
    assertTrue(load.snapshotForTime(2L) == null);
    assertTrue(load.snapshotForTime(3L) == s2);
  }

  @Test
  public void testGetAndMaybeCreateSnapshot() {
    Load load = new Load(3);

    Snapshot s1 = load.getAndMaybeCreateSnapshot(5L);
    Snapshot s2 = load.getAndMaybeCreateSnapshot(1L);
    Snapshot s3 = load.getAndMaybeCreateSnapshot(3L);

    assertEquals(3, load.numSnapshots());
    List<Snapshot> snapshots = load.snapshotsByTime();
    assertEquals(1L, snapshots.get(0).time());
    assertEquals(3L, snapshots.get(1).time());
    assertEquals(5L, snapshots.get(2).time());

    assertTrue(s1 == load.getAndMaybeCreateSnapshot(5L));
    assertTrue(s2 == load.getAndMaybeCreateSnapshot(1L));
    assertTrue(s3 == load.getAndMaybeCreateSnapshot(3L));
  }
}
