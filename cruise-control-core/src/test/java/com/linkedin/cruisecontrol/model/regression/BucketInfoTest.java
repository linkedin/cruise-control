/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.model.regression;

import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class BucketInfoTest {
  @Test
  public void testNoDataSample() {
    BucketInfo bucketInfo = new BucketInfo(5);
    assertEquals(0, bucketInfo.bucket(1.0f));
  }

  @Test
  public void testBucket() {
    BucketInfo bucketInfo = new BucketInfo(5);
    bucketInfo.record(11.0f);
    assertEquals(4, bucketInfo.bucket(21.0f));
    assertEquals(4, bucketInfo.bucket(19.0f));
    assertEquals(3, bucketInfo.bucket(17.0f));
    assertEquals(2, bucketInfo.bucket(15.0f));
    assertEquals(1, bucketInfo.bucket(13.0f));
    assertEquals(0, bucketInfo.bucket(11.0f));

    // Record a larger value
    bucketInfo.record(31.0f);
    assertEquals(4, bucketInfo.bucket(31.0f));
    assertEquals(4, bucketInfo.bucket(27.0f));
    assertEquals(3, bucketInfo.bucket(23.0f));
    assertEquals(2, bucketInfo.bucket(19.0f));
    assertEquals(1, bucketInfo.bucket(15.0f));
    assertEquals(0, bucketInfo.bucket(11.0f));

    // record a smaller value
    bucketInfo.record(1.0f);
    assertEquals(4, bucketInfo.bucket(31.0f));
    assertEquals(4, bucketInfo.bucket(25.5f));
    assertEquals(3, bucketInfo.bucket(19.5f));
    assertEquals(2, bucketInfo.bucket(13.5f));
    assertEquals(1, bucketInfo.bucket(7.0f));
    assertEquals(0, bucketInfo.bucket(1.0f));
  }


}
