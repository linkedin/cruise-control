/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.metric;

import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A Yammer metric processor that process the yammer metrics. Currently all the interested metrics are of type
 * Meter (BytesInRate, BytesOutRate) or Gauge (Partition Size).
 */
public class YammerMetricProcessor implements MetricProcessor<YammerMetricProcessor.Context> {
  private static final Logger LOG = LoggerFactory.getLogger(YammerMetricProcessor.class);

  @Override
  public void processMeter(MetricName metricName, Metered metered, Context context) throws Exception {
    if (MetricsUtils.isInterested(metricName)) {
      LOG.trace("Processing metric {} of type Meter.", metricName);
      double value;
      if (context.reportingInterval() <= 60000L) {
        value = metered.oneMinuteRate();
      } else if (context.reportingInterval() <= 300000) {
        value = metered.fiveMinuteRate();
      } else {
        value = metered.fifteenMinuteRate();
      }
      CruiseControlMetric ccm = MetricsUtils.toCruiseControlMetric(context.time(),
                                                                   context.brokerId(),
                                                                   metricName,
                                                                   value);
      context.reporter().sendCruiseControlMetric(ccm);
    }
  }

  @Override
  public void processCounter(MetricName metricName, Counter counter, Context context) throws Exception {
    if (MetricsUtils.isInterested(metricName)) {
      LOG.warn("Not processing metric {} of type Counter.", metricName);
    }
  }

  @Override
  public void processHistogram(MetricName metricName, Histogram histogram, Context context) throws Exception {
    if (MetricsUtils.isInterested(metricName)) {
      LOG.trace("Processing metric {} of type Histogram.", metricName);

      CruiseControlMetric ccm = MetricsUtils.toCruiseControlMetric(context.time(),
                                                                   context.brokerId(),
                                                                   metricName,
                                                                   histogram.max(),
                                                                   MetricsUtils.ATTRIBUTE_MAX);
      context.reporter().sendCruiseControlMetric(ccm);

      ccm = MetricsUtils.toCruiseControlMetric(context.time(),
                                               context.brokerId(),
                                               metricName,
                                               histogram.mean(),
                                               MetricsUtils.ATTRIBUTE_MEAN);
      context.reporter().sendCruiseControlMetric(ccm);
    }
  }

  @Override
  public void processTimer(MetricName metricName, Timer timer, Context context) throws Exception {
    if (MetricsUtils.isInterested(metricName)) {
      LOG.trace("Processing metric {} of type Timer.", metricName);

      CruiseControlMetric ccm = MetricsUtils.toCruiseControlMetric(context.time(),
                                                                   context.brokerId(),
                                                                   metricName,
                                                                   timer.fiveMinuteRate());
      context.reporter().sendCruiseControlMetric(ccm);

      ccm = MetricsUtils.toCruiseControlMetric(context.time(),
                                               context.brokerId(),
                                               metricName,
                                               timer.max(),
                                               MetricsUtils.ATTRIBUTE_MAX);
      context.reporter().sendCruiseControlMetric(ccm);

      ccm = MetricsUtils.toCruiseControlMetric(context.time(),
                                               context.brokerId(),
                                               metricName,
                                               timer.mean(),
                                               MetricsUtils.ATTRIBUTE_MEAN);
      context.reporter().sendCruiseControlMetric(ccm);
    }
  }

  @Override
  public void processGauge(MetricName metricName, Gauge<?> gauge, Context context) throws Exception {
    if (MetricsUtils.isInterested(metricName)) {
      LOG.trace("Processing metric {} of type Gauge.", metricName);
      if (!(gauge.value() instanceof Number)) {
        throw new IllegalStateException(String.format("The value of yammer metric %s is %s, which is not a number",
                                                      metricName, gauge.value()));
      }
      CruiseControlMetric ccm = MetricsUtils.toCruiseControlMetric(context.time(),
                                                                   context.brokerId(),
                                                                   metricName,
                                                                   ((Number) gauge.value()).doubleValue());
      context.reporter().sendCruiseControlMetric(ccm);
    }
  }

  public static final class Context {
    private final CruiseControlMetricsReporter _reporter;
    private final long _time;
    private final int _brokerId;
    private final long _reportingInterval;

    public Context(CruiseControlMetricsReporter reporter, long time, int brokerId, long reportingInterval) {
      _reporter = reporter;
      _time = time;
      _brokerId = brokerId;
      _reportingInterval = reportingInterval;
    }

    private CruiseControlMetricsReporter reporter() {
      return _reporter;
    }

    private long time() {
      return _time;
    }

    private int brokerId() {
      return _brokerId;
    }

    private long reportingInterval() {
      return _reportingInterval;
    }
  }
}
