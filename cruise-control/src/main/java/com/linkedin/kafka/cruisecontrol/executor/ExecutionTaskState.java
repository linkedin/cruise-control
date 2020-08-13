/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.executor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public enum ExecutionTaskState {
    PENDING, IN_PROGRESS, ABORTING, ABORTED, DEAD, COMPLETED;

    private static final List<ExecutionTaskState> CACHED_VALUES = Collections.unmodifiableList(Arrays.asList(values()));

    /**
     * Use this instead of values() because values() creates a new array each time.
     * @return enumerated values in the same order as values()
     */
    public static List<ExecutionTaskState> cachedValues() {
        return CACHED_VALUES;
    }
}
