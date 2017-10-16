/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol;

import com.linkedin.cruisecontrol.model.Entity;
import java.util.Objects;


public class IntegerEntity extends Entity<String> {
  private final String _group;
  private final int _id;

  public IntegerEntity(String group, int entityId) {
    _group = group;
    _id = entityId;
  }

  @Override
  public String group() {
    return _group;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_group, _id);
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (!(other instanceof IntegerEntity)) {
      return false;
    }
    IntegerEntity o = (IntegerEntity) other;
    return _id == o._id && _group.equals(o.group());
  }
}
