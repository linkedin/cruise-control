/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.cruisecontrol.common;

import java.util.Map;


/**
 * A Mix-in style interface for classes that are instantiated by reflection and need to take configuration parameters
 */
public interface CruiseControlConfigurable {

    /**
     * Configure this class with the given key-value pairs
     * @param configs Configurations.
     */
    void configure(Map<String, ?> configs);

}
