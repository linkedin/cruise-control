/*
 * Copyright 2024 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.persisteddata.namespace;

import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.persisteddata.ValueMappedMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Persisted data used by an {@link Executor} instance.
 */
public class ExecutorPersistedData {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutorPersistedData.class);

    /**
     * Keys to reference the particular broker map. For use with the "get" and "set" methods.
     */
    public enum BrokerMapKey {
        /**
         * Key to reference the "demote broker" map.
         */
        DEMOTE,

        /**
         * Key to reference the "remove broker" map.
         */
        REMOVE;
    }

    // Holds the maps for the demoted brokers and the removed brokers.
    private final Map<String, Map<Integer, Long>> _demotedOrRemovedBrokers;

    // Used to make thread safe changes to _demotedOrRemovedBrokers.
    private final Map<BrokerMapKey, Object> _locks = Map.of(
            BrokerMapKey.DEMOTE, new Object(),
            BrokerMapKey.REMOVE, new Object()
    );

    /**
     * Executor data that needs persisting is stored in the given map. It assumes that the map's
     * data is persisted independently. This class is concerned with providing a clean view of that
     * data and not with how it is stored.
     *
     * @param persistedMap The map to store {@link Executor} data in.
     */
    public ExecutorPersistedData(Map<String, String> persistedMap) {
        this._demotedOrRemovedBrokers = Namespace.EXECUTOR.embed(
                new ValueMappedMap<>(
                    persistedMap,
                    ExecutorPersistedData::serializeNumberMap,
                    ExecutorPersistedData::deserializeNumberMap));
    }

    /**
     * Package private for testing. Used to serialize a full number map to a single string.
     *
     * @param map Map data to serialize.
     * @return The string view of the map data.
     */
    static String serializeNumberMap(Object map) {
        if (!(map instanceof Map)) {
            return null;
        }

        return ((Map<?, ?>) map).entrySet().stream()
                .map(e -> e.getKey() + ":" + e.getValue())
                .collect(Collectors.joining(","));
    }

    /**
     * Package private for testing. Used to convert a map serialized by {@code serializeNumberMap}
     * back to an equivalent map.
     *
     * @param str The serialized map data to convert back into a map.
     * @return The deserialized string as a map or null if the data could not be deserialized.
     */
    static Map<Integer, Long> deserializeNumberMap(String str) {
        if (str == null) {
            return null;
        }
        if (str.isBlank()) {
            return new HashMap<>();
        }
        return Arrays.stream(str.split(","))
                .map(e -> {
                    final String[] parts = e.split(":");
                    if (parts.length != 2) {
                        return null;
                    }
                    try {
                        return Pair.of(Integer.parseInt(parts[0]), Long.parseLong(parts[1]));
                    } catch (NumberFormatException nfe) {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    /**
     * Gets the Demoted Brokers map or the Removed Brokers map depending on the key. If the map
     * hasn't been set yet, an empty map is returned. The returned map is a copy of the internal
     * data so any changes to it are not stored. Any changes to the map must be stored using
     * {@code setDemotedOrRemovedBrokers()}.
     *
     * @param key The key for which broker map to return.
     * @return Non-null broker map associated with the key.
     */
    public Map<Integer, Long> getDemotedOrRemovedBrokers(@Nonnull BrokerMapKey key) {
        final Map<Integer, Long> value = Objects.requireNonNullElseGet(
                this._demotedOrRemovedBrokers.get(key.toString()), HashMap::new);
        LOG.debug("Getting key={}, value={}", key, value);
        return value;
    }

    /**
     * Overwrite the stored broker map with the given map.
     *
     * @param key Key for the broker map to update.
     * @param newValue The new map of broker keys and values to store.
     */
    public void setDemotedOrRemovedBrokers(@Nonnull BrokerMapKey key, Map<Integer, Long> newValue) {
        LOG.debug("Setting key={}, value={}", key.toString(), newValue);
        this._demotedOrRemovedBrokers.put(key.toString(), newValue);
    }

    /**
     * Get the lock for the associated key so the broker maps can be updated with thread safety.
     *
     * @param key Key for the broker map to lock.
     * @return The object used to lock the associated broker map.
     */
    public Object getDemotedOrRemovedBrokersLock(BrokerMapKey key) {
        return this._locks.get(key);
    }

    /**
     * Modifies the requested broker map using the provided function. This ensures the map is
     * modified in a thread safe manner.
     *
     * @param key The key of the specific map to modify.
     * @param mapModifier Used to modify the current values of the requested map.
     * @param <R> The return value resulting from applying {@code mapModifier} to the current map.
     * @return The value returned by the {@code mapModifier} function.
     */
    public <R> R modifyDemotedOrRemovedBrokers(BrokerMapKey key,
            @Nonnull Function<Map<Integer, Long>, R> mapModifier) {
        R result;
        synchronized (this.getDemotedOrRemovedBrokersLock(key)) {
            final Map<Integer, Long> currentValue = this.getDemotedOrRemovedBrokers(key);
            result = mapModifier.apply(currentValue);
            this.setDemotedOrRemovedBrokers(key, currentValue);
        }
        return result;
    }

}
