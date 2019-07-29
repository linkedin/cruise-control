/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * The broker capacity config resolver implementation based on files. The format of the file is JSON:
 * <pre>
 *   {
 *      "brokerCapacities":[
 *        {
 *          "brokerId": "-1",
 *          "capacity": {
 *            "DISK": "1000000",
 *            "CPU": "100",
 *            "NW_IN": "100000",
 *            "NW_OUT": "100000"
 *          }
 *        },
 *        {
 *          "brokerId": "0",
 *          "capacity": {
 *            "DISK": "1000000",
 *            "CPU": "100",
 *            "NW_IN": "100000",
 *            "NW_OUT": "100000"
 *          }
 *        },
 *        {
 *          "brokerId": "1",
 *          "capacity": {
 *            "DISK": "1000000",
 *            "CPU": "100",
 *            "NW_IN": "100000",
 *            "NW_OUT": "100000"
 *          }
 *        }
 *      ]
 *   }
 * </pre>
 *
 * Example capacity file with number of cores (i.e. see the use of {@link #NUM_CORES_CONFIG} config):
 * <pre>
 * {
 *   "brokerCapacities":[
 *     {
 *       "brokerId": "-1",
 *       "capacity": {
 *         "DISK": "1000000",
 *         "CPU": {"num.cores": "16"},
 *         "NW_IN": "10000",
 *         "NW_OUT": "10000"
 *       }
 *     },
 *     {
 *       "brokerId": "0",
 *       "capacity": {
 *         "DISK": "1000000",
 *         "CPU": {"num.cores": "32"},
 *         "NW_IN": "50000",
 *         "NW_OUT": "50000"
 *       }
 *     }
 *   ]
 * }
 * </pre>
 *
 * The broker id -1 defines the default broker capacity estimate. A broker capacity is overridden if there is a capacity
 * defined for a particular broker id. In case a broker capacity is missing, the default estimate for a broker capacity
 * will be used.
 *
 * The units of the definition are:
 * <ul>
 *  <li>DISK - MB</li>
 *  <li>CPU - either (1) Percentage (0 - 100) or (2) number of cores (see the use of {@link #NUM_CORES_CONFIG} config) </li>
 *  <li>NW_IN - KB/s</li>
 *  <li>NW_OUT - KB/s</li>
 * </ul>
 */
public class BrokerCapacityConfigFileResolver implements BrokerCapacityConfigResolver {
  public static final String CAPACITY_CONFIG_FILE = "capacity.config.file";
  public static final int DEFAULT_CAPACITY_BROKER_ID = -1;
  private static final String NUM_CORES_CONFIG = "num.cores";
  public static final double DEFAULT_CPU_CAPACITY_WITH_CORES = 100.0;
  private static Map<Integer, BrokerCapacityInfo> _capacitiesForBrokers;

  @Override
  public void configure(Map<String, ?> configs) {
    String configFile = KafkaCruiseControlUtils.getRequiredConfig(configs, CAPACITY_CONFIG_FILE);
    try {
      loadCapacities(configFile);
    } catch (FileNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public BrokerCapacityInfo capacityForBroker(String rack, String host, int brokerId) {
    if (brokerId >= 0) {
      BrokerCapacityInfo capacity = _capacitiesForBrokers.get(brokerId);
      if (capacity != null) {
        return capacity;
      } else {
        String info = String.format("Missing broker id(%d) in capacity config file.", brokerId);
        return new BrokerCapacityInfo(_capacitiesForBrokers.get(DEFAULT_CAPACITY_BROKER_ID).capacity(), info,
                                      _capacitiesForBrokers.get(DEFAULT_CAPACITY_BROKER_ID).numCpuCores());
      }
    } else {
      throw new IllegalArgumentException("The broker id(" + brokerId + ") should be non-negative.");
    }
  }

  /**
   * Get the number of cores specified by user -- i.e. requires the {@link Resource#CPU} capacity to specify
   * {@link #NUM_CORES_CONFIG} in a Map.
   *
   * @param brokerCapacity Broker capacity for each resource.
   * @return Number of cores if specified by user, {@code null} otherwise.
   */
  @SuppressWarnings("unchecked")
  private static Short getUserSpecifiedNumCores(Map<Resource, Object> brokerCapacity) {
    if (brokerCapacity.get(Resource.CPU) instanceof Map) {
      String stringNumCores = ((Map<String, String>) brokerCapacity.get(Resource.CPU)).get(NUM_CORES_CONFIG);
      if (stringNumCores == null) {
        throw new IllegalArgumentException("Missing " + NUM_CORES_CONFIG + " config for brokers in capacity config file.");
      }
      return Short.parseShort(stringNumCores);
    } else {
      return null;
    }
  }

  private static Map<Resource, Double> getTotalCapacity(Map<Resource, Object> brokerCapacity, boolean hasNumCores) {
    Map<Resource, Double> totalCapacity = new HashMap<>(brokerCapacity.size());
    brokerCapacity.forEach((key, value) -> totalCapacity.put(key, hasNumCores && key == Resource.CPU ? DEFAULT_CPU_CAPACITY_WITH_CORES
                                                                                                     : Double.parseDouble((String) value)));
    return totalCapacity;
  }

  private static void numCoresConfigConsistencyChecker(Set<Boolean> numCoresConfigConsistency) {
    if (numCoresConfigConsistency.size() > 1) {
      throw new IllegalArgumentException("Inconsistent " + NUM_CORES_CONFIG + " config for brokers in capacity config file. This "
                                         + "config must be provided by either all or non of the brokers.");
    }
  }

  private BrokerCapacityInfo getBrokerCapacityInfo(BrokerCapacity bc, Set<Boolean> numCoresConfigConsistency) {
    Short userSpecifiedNumCores = getUserSpecifiedNumCores(bc.capacity);
    boolean hasNumCores = userSpecifiedNumCores != null;
    numCoresConfigConsistency.add(hasNumCores);
    numCoresConfigConsistencyChecker(numCoresConfigConsistency);
    boolean isDefault = bc.brokerId == DEFAULT_CAPACITY_BROKER_ID;
    Map<Resource, Double> totalCapacity = getTotalCapacity(bc.capacity, hasNumCores);

    BrokerCapacityInfo brokerCapacityInfo;
    if (hasNumCores) {
      brokerCapacityInfo
          = isDefault ? new BrokerCapacityInfo(totalCapacity, "The default broker capacity.", userSpecifiedNumCores)
                      : new BrokerCapacityInfo(totalCapacity, userSpecifiedNumCores);
    } else {
      brokerCapacityInfo
          = isDefault ? new BrokerCapacityInfo(totalCapacity, "The default broker capacity.")
                      : new BrokerCapacityInfo(totalCapacity);
    }
    return brokerCapacityInfo;
  }

  private void loadCapacities(String capacityConfigFile) throws FileNotFoundException {
    JsonReader reader = null;
    try {
      reader = new JsonReader(new InputStreamReader(new FileInputStream(capacityConfigFile), StandardCharsets.UTF_8));
      Gson gson = new Gson();
      Set<BrokerCapacity> brokerCapacities = ((BrokerCapacities) gson.fromJson(reader, BrokerCapacities.class)).brokerCapacities;
      _capacitiesForBrokers = new HashMap<>(brokerCapacities.size());
      Set<Boolean> numCoresConfigConsistency = new HashSet<>(1);
      for (BrokerCapacity bc : brokerCapacities) {
        _capacitiesForBrokers.put(bc.brokerId, getBrokerCapacityInfo(bc, numCoresConfigConsistency));
      }
    } finally {
      try {
        if (reader != null) {
          reader.close();
        }
      } catch (IOException e) {
        // let it go.
      }
    }
  }

  @Override
  public void close() throws Exception {
    // nothing to do.
  }

  private static class BrokerCapacities {
    private Set<BrokerCapacity> brokerCapacities;
  }

  private static class BrokerCapacity {
    private final int brokerId;
    private final Map<Resource, Object> capacity;

    BrokerCapacity(int brokerId, Map<Resource, Object> capacity) {
      this.brokerId = brokerId;
      this.capacity = capacity;
    }
  }
}
