/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.model;

import com.linkedin.cruisecontrol.exception.ModelInputException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public class GroupRegistry {

  private final Map<String, Group> _registeredGroupsById;

  public GroupRegistry() {
    _registeredGroupsById = new HashMap<>();
  }

  /**
   * Register a new group to the Group Registry.
   *
   * @param groupToRegister A new group to register.
   * @throws ModelInputException Cannot register a group with the same id multiple times.
   */
  void registerGroup(Group groupToRegister) throws ModelInputException {
    boolean isAlreadyRegistered = _registeredGroupsById.putIfAbsent(groupToRegister.id(), groupToRegister) != null;
    // Sanity check: Cannot register a group with the same id multiple times.
    if (isAlreadyRegistered) {
      throw new ModelInputException("The group with id " + groupToRegister.id() + " has already been registered.");
    }
  }

  /**
   * Get the set of ids for groups.
   */
  Set<String> ids() {
    return _registeredGroupsById.keySet();
  }

  /**
   * Get the group with the given id.
   *
   * @param id Id corresponding to the requested group.
   * @return Group with the given id, or null if a group with the given id does not exist.
   */
  Group group(String id) {
    return _registeredGroupsById.get(id);
  }

  /**
   * Clear the groups in the group registry.
   */
  void clear() {
    _registeredGroupsById.values().forEach(Group::clear);
    _registeredGroupsById.clear();
  }
}