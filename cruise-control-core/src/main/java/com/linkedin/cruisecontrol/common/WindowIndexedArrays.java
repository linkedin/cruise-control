/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.common;

/**
 * This class helps maintain the window indices in circular arrays.
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
   * @return The length of the array that is being maintained. This should include both stable and active windows.
   */
  protected abstract int length();

  /**
   * Check if a given window index is valid or not. A valid window index is between the oldest window index(inclusive)
   * and current window index(exclusive).
   *
   * @param windowIndex the window index to check.
   */
  protected void validateWindowIndex(long windowIndex) {
    if (!inValidWindowRange(windowIndex)) {
      throw new IllegalArgumentException(String.format("Index %d is out of range [%d, %d]", windowIndex,
                                                       _oldestWindowIndex, lastWindowIndex()));
    }
  }

  /**
   * The previous array index of a given array index. This method handles the wrapping at the circular buffer edge.
   * @param arrayIndex the array index to get the previous array index for.
   * @return The previous array index if exists, {@link #INVALID_INDEX} otherwise.
   */
  protected int prevArrayIndex(int arrayIndex) {
    return arrayIndex == firstArrayIndex() ? INVALID_INDEX : (arrayIndex + length() - 1) % length();
  }

  /**
   * The next array index of a given array index. This method handles the wrapping at the circular buffer edge.
   * @param arrayIndex the array index to get the next array index for.
   * @return The next array index if exists, {@link #INVALID_INDEX} otherwise.
   */
  protected int nextArrayIndex(int arrayIndex) {
    return arrayIndex == lastArrayIndex() ? INVALID_INDEX : (arrayIndex + 1) % length();
  }

  /**
   * Get the first array index in the circular array. The first array index is the array index of the oldest window
   * index.
   * @return The first array index in the circular array.
   */
  protected int firstArrayIndex() {
    return arrayIndex(_oldestWindowIndex);
  }

  /**
   * @return The last array index in the circular array corresponding to {@link #lastWindowIndex()}.
   */
  protected int lastArrayIndex() {
    return arrayIndex(lastWindowIndex());
  }

  /**
   * @return The last stable window index -- i.e. {@link #currentWindowIndex()} - 1.
   */
  protected long lastWindowIndex() {
    return currentWindowIndex() - 1;
  }

  /**
   * Check if a given window index is valid or not. A valid window index is between the oldest window index(inclusive)
   * and current window index(exclusive).
   *
   * @param windowIndex the window index to check.
   * @return {@code true} if the window index is valid, {@code false} otherwise.
   */
  protected boolean inValidWindowRange(long windowIndex) {
    return windowIndex >= _oldestWindowIndex && windowIndex <= lastWindowIndex();
  }

  /**
   * @return The current window index.
   */
  protected long currentWindowIndex() {
    return _oldestWindowIndex + length() - 1;
  }

  /**
   * Get the array index of the given window index.
   * @param windowIndex the window index to get the array index for.
   *
   * @return The array index of the given window index.
   */
  protected int arrayIndex(long windowIndex) {
    return (int) (windowIndex % length());
  }
}
