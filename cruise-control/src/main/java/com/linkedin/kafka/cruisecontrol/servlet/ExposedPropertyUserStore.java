/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import org.eclipse.jetty.security.DefaultIdentityService;
import org.eclipse.jetty.security.IdentityService;
import org.eclipse.jetty.security.PropertyUserStore;
import org.eclipse.jetty.security.RolePrincipal;
import org.eclipse.jetty.security.UserIdentity;
import org.eclipse.jetty.security.UserPrincipal;
import javax.security.auth.Subject;
import java.util.List;
import java.util.Set;

public class ExposedPropertyUserStore extends PropertyUserStore {

  private IdentityService _identityService = new DefaultIdentityService();

  /**
   * This method exposes the protected `_users` map inherited from
   * UserStore, providing direct access to the stored users' names.
   * @return the set of users' names
   */
  public Set<String> getUsersNames() {
        return _users.keySet();
    }

  /**
   * This method gets the UserIdentity for a given username.
   * @param userName the user's name
   * @return the UserIdentity
   */
  public UserIdentity getUserIdentity(String userName) {
    UserPrincipal user = super.getUserPrincipal(userName);
    List<RolePrincipal> roles = super.getRolePrincipals(userName);
    if (user == null) {
      return null;
    }
    return _identityService.newUserIdentity(
      createSubject(user, roles),
      user,
      roles.stream().map(RolePrincipal::getName).toArray(String[]::new)
    );
  }

  private Subject createSubject(UserPrincipal user, List<RolePrincipal> rolePrincipals) {
    Subject subject = new Subject();
    subject.getPrincipals().add(user);
    subject.getPrincipals().addAll(rolePrincipals);
    return subject;
  }
}
