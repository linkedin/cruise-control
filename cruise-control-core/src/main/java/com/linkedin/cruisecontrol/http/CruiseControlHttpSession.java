/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.http;

/**
 * Http session for Cruise Control.
 */
public interface CruiseControlHttpSession {

    void invalidateSession();

    long getLastAccessed();

    String getSessionId();
}
