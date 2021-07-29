/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.httframeworkhandler;

public interface CruiseControlHttpSession {

    void invalidateSession();

    long getLastAccessed();

    String getSessionId();
}
