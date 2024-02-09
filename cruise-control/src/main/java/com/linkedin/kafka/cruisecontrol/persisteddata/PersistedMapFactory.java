/*
 * Copyright 2024 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.persisteddata;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.PersistedDataConfig;
import com.linkedin.kafka.cruisecontrol.persisteddata.kafka.KafkaPersistedMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.AdminClient;

import static com.linkedin.kafka.cruisecontrol.persisteddata.PersistMethod.KAFKA;
import static com.linkedin.kafka.cruisecontrol.persisteddata.PersistMethod.MEMORY;

/**
 * Constructs the correct {@link PersistedMap} implementation based on the provided
 * {@link KafkaCruiseControlConfig}. In particular, it uses the value of
 * {@link PersistedDataConfig#PERSIST_METHOD_CONFIG} to determine which implementation to
 * construct.
 */
public class PersistedMapFactory {

    // The overall configuration is used when creating the {@link PersistedMap} implementation.
    private final KafkaCruiseControlConfig _config;

    // Keeps the suppliers for implementation-specific instances.
    private final Map<PersistMethod, Supplier<PersistedMap>> _suppliers;

    /**
     * Creates an instance that is able to construct the correct {@link PersistedMap}
     * implementation.
     *
     * @param config The complete program configuration to evaluate. Specifically, the
     * {@link PersistedDataConfig#PERSIST_METHOD_CONFIG} config is needed.
     * @param adminClient The admin client to pass to {@link KafkaPersistedMap}, if needed.
     */
    public PersistedMapFactory(KafkaCruiseControlConfig config, AdminClient adminClient) {
        this(config,
                () -> new KafkaPersistedMap(config, adminClient),
                () -> new PersistedMap(new ConcurrentHashMap<>()));
    }

    /**
     * Creates an instance that is able to construct the correct {@link PersistedMap}
     * implementation.
     *
     * @param kafkaSupplier The supplier for {@link KafkaPersistedMap}.
     * @param memoryAndDefaultSupplier The supplier for {@link PersistedMap} and the default
     * implementation.
     */
    PersistedMapFactory(KafkaCruiseControlConfig config, Supplier<PersistedMap> kafkaSupplier,
            Supplier<PersistedMap> memoryAndDefaultSupplier) {
        this._config = config;
        this._suppliers = Map.of(
                KAFKA, kafkaSupplier,
                MEMORY, memoryAndDefaultSupplier);
    }

    /**
     * Constructs the correct {@link PersistedMap} implementation based on the configured
     * {@link PersistedDataConfig#PERSIST_METHOD_CONFIG}.
     *
     * @return The {@link PersistedMap} implementation.
     */
    public PersistedMap instance() {
        PersistMethod backingMethod = PersistMethod.fromString(
                _config.getString(PersistedDataConfig.PERSIST_METHOD_CONFIG));
        return this._suppliers.getOrDefault(backingMethod, this._suppliers.get(MEMORY)).get();
    }
}
