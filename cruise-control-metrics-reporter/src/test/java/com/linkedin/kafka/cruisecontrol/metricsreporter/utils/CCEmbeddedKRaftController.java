/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.metricsreporter.utils;

import kafka.server.KafkaConfig;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class CCEmbeddedKRaftController implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(CCEmbeddedKRaftController.class);
    private static final String HOST = "localhost";
    private static final int ID = 100;
    private int _port = 0;
    private final File _logDir;
    private final String _clusterId;
    private final CCKafkaRaftServer _kafkaServer;

    public CCEmbeddedKRaftController() {
        _logDir = CCKafkaTestUtils.newTempDir();
        _clusterId = generateUuidAsBase64();
        KafkaConfig kafkaConfig = new KafkaConfig(createControllerProperties(), true);
        _kafkaServer = new CCKafkaRaftServer(kafkaConfig, _clusterId, Time.SYSTEM);
    }

    @Override
    public void close() throws Exception {
        CCKafkaTestUtils.quietly(this::shutdown);
        CCKafkaTestUtils.quietly(this::awaitShutdown);
        CCKafkaTestUtils.quietly(() -> FileUtils.forceDelete(_logDir));
    }

    /**
     * Startup
     */
    public void startup() {
        LOG.info("Starting Kraft Server.");
        _kafkaServer.startup();
        try {
            _port = _kafkaServer.boundControllerPort();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException("Cannot bound KRaft Controller port", e);
        }
    }

    /**
     * Shutdown
     */
    public void shutdown() {
        LOG.info("Shutdown initiated for KRaft Server.");
        _kafkaServer.shutdown();
    }

    /**
     * Await shutdown completed
     */
    public void awaitShutdown() {
        _kafkaServer.awaitShutdown();
    }

    /**
     * Get KRaft quorum voters (id, host, port)
     * @return KRaft quorum voters string
     */
    public String quorumVoters() {
        return ID + "@" + HOST + ":" + _port;
    }

    /**
     * Get KRaft cluster ID
     * @return the ID of the Kafka cluster where this controller belongs to
     */
    public String clusterId() {
        return _clusterId;
    }

    private Properties createControllerProperties() {
        Properties props = new Properties();
        props.setProperty("process.roles", "controller");
        props.setProperty("node.id", String.valueOf(ID));
        props.setProperty("controller.listener.names", "CONTROLLER");
        props.setProperty("listeners", "CONTROLLER://:" + _port);
        props.setProperty("controller.quorum.voters", quorumVoters());
        props.setProperty("log.dir", _logDir.getAbsolutePath());
        props.setProperty("metadata.log.dir", _logDir.getAbsolutePath());
        return props;
    }

    private String generateUuidAsBase64() {
        UUID uuid = UUID.randomUUID();
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
        byteBuffer.putLong(uuid.getMostSignificantBits());
        byteBuffer.putLong(uuid.getLeastSignificantBits());
        return Base64.getUrlEncoder().withoutPadding().encodeToString(byteBuffer.array());
    }
}
