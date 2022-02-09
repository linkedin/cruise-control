/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.linkedin.cruisecontrol.servlet.EndPoint;
import com.linkedin.kafka.cruisecontrol.async.AsyncKafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager;
import com.linkedin.kafka.cruisecontrol.servlet.purgatory.Purgatory;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.KAFKA_CRUISE_CONTROL_SERVLET_SENSOR;

public class KafkaCruiseControlEndPoints {
    protected final AsyncKafkaCruiseControl _asyncKafkaCruiseControl;
    protected final KafkaCruiseControlConfig _config;
    protected final UserTaskManager _userTaskManager;
    protected final ThreadLocal<Integer> _asyncOperationStep;
    protected final Map<EndPoint, Meter> _requestMeter = new HashMap<>();
    protected final Map<EndPoint, Timer> _successfulRequestExecutionTimer = new HashMap<>();
    protected final boolean _twoStepVerification;
    protected final Purgatory _purgatory;

    public KafkaCruiseControlEndPoints(AsyncKafkaCruiseControl asynckafkaCruiseControl, MetricRegistry dropwizardMetricRegistry) {
        this(asynckafkaCruiseControl, dropwizardMetricRegistry, null);
    }

    // Visible for testing
    public KafkaCruiseControlEndPoints(AsyncKafkaCruiseControl asynckafkaCruiseControl,
                                       MetricRegistry dropwizardMetricRegistry, UserTaskManager userTaskManager) {
        _config = asynckafkaCruiseControl.config();
        _asyncKafkaCruiseControl = asynckafkaCruiseControl;
        _twoStepVerification = _config.getBoolean(WebServerConfig.TWO_STEP_VERIFICATION_ENABLED_CONFIG);
        _purgatory = _twoStepVerification ? new Purgatory(_config) : null;
        if (userTaskManager == null) {
            _userTaskManager = new UserTaskManager(_config, dropwizardMetricRegistry, _successfulRequestExecutionTimer, _purgatory);
        } else {
            _userTaskManager = userTaskManager;
        }
        _asyncKafkaCruiseControl.setUserTaskManagerInExecutor(_userTaskManager);
        _asyncOperationStep = new ThreadLocal<>();
        _asyncOperationStep.set(0);
        for (CruiseControlEndPoint endpoint : CruiseControlEndPoint.cachedValues()) {
            _requestMeter.put(endpoint, dropwizardMetricRegistry.meter(
                    MetricRegistry.name(KAFKA_CRUISE_CONTROL_SERVLET_SENSOR, endpoint.name() + "-request-rate")));
            _successfulRequestExecutionTimer.put(endpoint, dropwizardMetricRegistry.timer(
                    MetricRegistry.name(KAFKA_CRUISE_CONTROL_SERVLET_SENSOR, endpoint.name() + "-successful-request-execution-timer")));
        }
    }

    public AsyncKafkaCruiseControl asyncKafkaCruiseControl() {
        return _asyncKafkaCruiseControl;
    }

    public Map<EndPoint, Timer> successfulRequestExecutionTimer() {
        return Collections.unmodifiableMap(_successfulRequestExecutionTimer);
    }

    public ThreadLocal<Integer> asyncOperationStep() {
        return _asyncOperationStep;
    }

    public Purgatory purgatory() {
        return _purgatory;
    }

    public UserTaskManager userTaskManager() {
        return _userTaskManager;
    }

    public KafkaCruiseControlConfig config() {
        return _config;
    }

    public Map<EndPoint, Meter> requestMeter() {
        return _requestMeter;
    }

    /**
     *
     * @return is_twoStepVerification
     */
    public boolean twoStepVerification() {
        return _twoStepVerification;
    }

    /**
     * Destroys the UserTaskManager and the Purgatory.
     */
    public void destroy() {
        _userTaskManager.close();
        if (_purgatory != null) {
            _purgatory.close();
        }
    }

    public List<UserTaskManager.UserTaskInfo> getAllUserTasks() {
        return _userTaskManager.getAllUserTasks();
    }
}
