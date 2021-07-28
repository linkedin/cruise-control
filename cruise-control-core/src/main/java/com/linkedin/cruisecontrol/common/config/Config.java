/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.cruisecontrol.common.config;

import java.util.Collections;
import java.util.List;


public class Config {
    private final List<ConfigValue> _configValues;

    public Config(List<ConfigValue> configValues) {
        this._configValues = configValues;
    }

    public List<ConfigValue> configValues() {
        return Collections.unmodifiableList(_configValues);
    }
}
