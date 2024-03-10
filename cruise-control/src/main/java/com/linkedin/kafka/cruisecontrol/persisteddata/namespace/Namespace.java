/*
 * Copyright 2024 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.persisteddata.namespace;

import com.linkedin.kafka.cruisecontrol.persisteddata.KeyMappedMap;
import java.util.Locale;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Allows multiple use cases by keeping them cleanly and consistently separated. New use cases can
 * be added by adding their own enum constant.
 */
public enum Namespace {
    // The defined namespaces. There needs to be a unique one for each use case.
    EXECUTOR;

    // This is the cached string version of the name of the enum used as the map key prefix.
    private final String _prefix;

    Namespace() {
        this._prefix = this.name().toLowerCase(Locale.ROOT) + ".";
    }

    /**
     * Embed the namespace into the provided map. The returned map is a wrapper around the provided
     * map.
     *
     * @param backingStore The map to embed the namespace into.
     * @param <V> The type of values stored in the map.
     * @return A wrapped view of the provided map that keeps keys and values in their own namespace.
     */
    public <V> Map<String, V> embed(Map<String, V> backingStore) {
        return new KeyMappedMap<>(
                backingStore,
                this::keyToNamespaceMapper,
                this::namespaceToKeyMapper);
    }

    // Package private for testing.
    @Nonnull
    String keyToNamespaceMapper(Object key) {
        return this._prefix + key;
    }

    // Package private for testing.
    String namespaceToKeyMapper(@Nonnull String namespace) {
        return namespace.startsWith(this._prefix)
                ? namespace.substring(this._prefix.length())
                : null;
    }
}
