/*
 * Copyright 2024 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.persisteddata;

import java.util.Map;
import javax.annotation.Nonnull;

public class PersistedMap extends MapDecorator<String, String> {

    /**
     * Wraps the child map so accesses to it can be decorated.
     *
     * @param child The other map instance to decorate.
     */
    public PersistedMap(Map<String, String> child) {
        super(child);
    }

    @Override
    public String remove(@Nonnull Object key) {
        return this.put(key.toString(), null);
    }

    @Override
    public void putAll(@Nonnull Map<? extends String, ? extends String> map) {
        map.forEach(this::put);
    }

    @Override
    public void clear() {
        // This only really works if other write operations have stopped.
        this.keySet().forEach(this::remove);
    }
}
