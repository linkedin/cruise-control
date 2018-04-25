/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.common;

/**
 * This class helps maintain the window indexes in circular arrays.
 *
 * The default methods assumes there are N + 1 windows, where there are N stable windows and one current active window.
 * The active window is not considered as visible to the users, i.e. it is not a valid window index to query.
 */
public abstract class WindowIndexedArrays {
  public static final int INVALID_INDEX = -1;
  // The oldest window index.
  protected volatile long _oldestWindowIndex;

  /**
   * Update the oldest window index. This usually happens when a new window is rolled out.
   * The oldest window index should be monotonically increasing.
   *
   * @param newOldestWindowIndex the new oldest window index.
   */
  public void updateOldestWindowIndex(long newOldestWindowIndex) {
    if (_oldestWindowIndex >= newOldestWindowIndex) {
      throw new IllegalArgumentException("The new oldest window " + newOldestWindowIndex + " is no larger than "
                                             + "the current oldest window " + _oldestWindowIndex);
    }
    _oldestWindowIndex = newOldestWindowIndex;
  }

  /**
   * @return the length of the array that is being maintained. This should include both stable and active windows.
   */
  protected abstract int length();

  /**
   * Check if a given window index is valid or not. A valid window index is between the oldest window index(inclusive)
   * and current window index(exclusive).
   *
   * @param windowIndex the window index to check.
   */
  protected void validateIndex(long windowIndex) {
    if (!inValidRange(windowIndex)) {
      throw new IllegalArgumentException(String.format("Index %d is out of range [%d, %d]", windowIndex,
                                                       _oldestWindowIndex, currentWindowIndex() - 1));
    }
  }

  /**
   * The previous array index of a given array index. This method handles the wrapping at the circular buffer edge.
   * @param idx the array index to get the previous array index for.
   * @return the previous array index if exists, {@link #INVALID_INDEX} otherwise.
   */
  protected int prevIdx(int idx) {
    return idx == firstIdx() ? INVALID_INDEX : (idx + length() - 1) % length();
  }

  /**
   * The next array index of a given array index. This method handles the wrapping at the circular buffer edge.
   * @param idx the array index to get the next array index for.
   * @return the next array index if exists, {@link #INVALID_INDEX} otherwise.
   */
  protected int nextIdx(int idx) {
    return idx == lastIdx() ? INVALID_INDEX : (idx + 1) % length();
  }

  /**
   * Get the first array index in the circular array. The first array index is the array index of the oldest window
   * index.
   * @return the first array index in the circular array.
   */
  protected int firstIdx() {
    return arrayIndex(_oldestWindowIndex);
  }

  /**
   * Get the last array index in the circular array. The last index is the array index of last stable window index.
   * i.e. the array index of (current_window_index - 1)
   *
   * @return the last index in the circular array.
   */
  protected int lastIdx() {
    return arrayIndex(currentWindowIndex() - 1);
  }

  /**
   * Check if a given window index is valid or not. A valid window index is between the oldest window index(inclusive)
   * and current window index(exclusive).
   *
   * @param windowIndex the window index to check.
   * @return true if the window index is valid, false otherwise.
   */
  protected boolean inValidRange(long windowIndex) {
    return windowIndex >= _oldestWindowIndex && windowIndex <= currentWindowIndex() - 1;
  }

  /**
   * @return the current window index.
   */
  protected long currentWindowIndex() {
    return _oldestWindowIndex + length() - 1;
  }

  /**
   * Get the array index of the given window index.
   * @param windowIndex the window index to get the array index for.
   *
   * @return the array index of the given window index.
   */
  protected int arrayIndex(long windowIndex) {
    return (int) (windowIndex % length());
  }
}
