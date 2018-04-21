/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.model.regression;

import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * A class holding the metric info and index.
 * 
 * Since the metric id of each metric defined in the {@link CausalRelation} may not always be contiguous, the causal 
 * relation needs to assign an index to each metric. This class maintains the metrics to the assigned index mapping.  
 */
public class CausalRelation {
  private final Set<MetricInfo> _resultantMetrics;
  private final Set<MetricInfo> _causalMetrics;
  private final Map<MetricInfo, Integer> _indexes;

  CausalRelation(Collection<MetricInfo> resultantMetrics, Collection<MetricInfo> causalMetrics) {
    _resultantMetrics = new HashSet<>(resultantMetrics);
    _causalMetrics = new HashSet<>(causalMetrics);
    _indexes = new HashMap<>();
    int i = 0;
    // First add the causal metrics. We do this so that the index in the causal relation is the same as the
    // index of the coefficient in the linear regression.
    for (MetricInfo causalMetric : _causalMetrics) {
      _indexes.put(causalMetric, i);
      i++;
    }
    // Then add the the resultant metrics.
    for (MetricInfo resultantMetric : resultantMetrics) {
      if (causalMetrics.contains(resultantMetric)) {
        throw new IllegalArgumentException(
            String.format("The resultant metric %s cannot be in the causal metrics %s", resultantMetric,
                          _causalMetrics));
      }
      _indexes.put(resultantMetric, i);
      i++;
    }
  }

  int index(MetricInfo metricInfo) {
    return _indexes.get(metricInfo);
  }

  Map<MetricInfo, Integer> indexes() {
    return _indexes;
  }

  public Set<MetricInfo> resultantMetrics() {
    return Collections.unmodifiableSet(_resultantMetrics);
  }

  public Set<MetricInfo> causalMetrics() {
    return Collections.unmodifiableSet(_causalMetrics);
  }
}
