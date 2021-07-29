/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.vertx;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.CruiseControlEndPoints;
import com.linkedin.kafka.cruisecontrol.RequestHandler;
import com.linkedin.kafka.cruisecontrol.async.AsyncKafkaCruiseControl;
import io.vertx.ext.web.RoutingContext;
import java.io.IOException;

public class VertxRequestHandler {

    private final RequestHandler _requestHandler;

    public VertxRequestHandler(AsyncKafkaCruiseControl asynckafkaCruiseControl, MetricRegistry dropwizardMetricRegistry) {
        _requestHandler = new RequestHandler(asynckafkaCruiseControl, dropwizardMetricRegistry);
    }

    /**
     * Shuts down the handler.
     */
    public void destroy() {
        _requestHandler.destroy();
    }

    /**
     * Handles the request
     * @param context is the request
     */
    public void handle(RoutingContext context) {
        try {
            _requestHandler.doGetOrPost(new VertxRequestContext(context, cruiseControlEndPoints().config()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public CruiseControlEndPoints cruiseControlEndPoints() {
        return _requestHandler.cruiseControlEndPoints();
    }
}
