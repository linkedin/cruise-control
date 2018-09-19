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
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * The broker capacity config resolver implementation based on files. The format of the file is JSON. Depending on
 * whether the JBOD configuration is used or not, this JSON file may specify disk capacity per logDir.
 * Example capacity file without JBOD:
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
 * Example capacity file with JBOD (provided logDirs must be absolute paths):
 * <pre>
 *   {
 *      "brokerCapacities":[
 *        {
 *          "brokerId": "-1",
 *          "capacity": {
 *            "DISK": {"/tmp/kafka-logs-1": "400000", "/tmp/kafka-logs-2": "200000", "/tmp/kafka-logs-3": "200000",
 *           "/tmp/kafka-logs-4": "200000", "/tmp/kafka-logs-5": "200000", "/tmp/kafka-logs-6": "200000"},
 *            "CPU": "100",
 *            "NW_IN": "100000",
 *            "NW_OUT": "100000"
 *          }
 *        },
 *        {
 *          "brokerId": "0",
 *          "capacity": {
 *            "DISK": {"/tmp/kafka-logs-1": "350000", "/tmp/kafka-logs-2": "550000"},
 *            "CPU": "100",
 *            "NW_IN": "100000",
 *            "NW_OUT": "100000"
 *          }
 *        },
 *        {
 *          "brokerId": "1",
 *          "capacity": {
 *            "DISK": {"/tmp/kafka-logs": "2000000"},
 *            "CPU": "100",
 *            "NW_IN": "100000",
 *            "NW_OUT": "100000"
 *          }
 *        }
 *      ]
 *   }
 * </pre>
 *
 * The broker id -1 defines the default broker capacity estimate. A broker capacity is overridden if there is a capacity
 * defined for a particular broker id. In case a broker capacity is missing, the default estimate for a broker capacity
 * will be used.
 *
 * The units of the definition are:
 * <ul>
 *  <li>DISK - MB</li>
 *  <li>CPU - Percent</li>
 *  <li>NW_IN - KB/s</li>
 *  <li>NW_OUT - KB/s</li>
 * </ul>
 */
public class BrokerCapacityConfigFileResolver implements BrokerCapacityConfigResolver {
  public static final String CAPACITY_CONFIG_FILE = "capacity.config.file";
  public static final int DEFAULT_CAPACITY_BROKER_ID = -1;
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
                                      _capacitiesForBrokers.get(DEFAULT_CAPACITY_BROKER_ID).diskCapacityByLogDir());
      }
    } else {
      throw new IllegalArgumentException("The broker id(" + brokerId + ") should be non-negative.");
    }
  }

  private static boolean isJBOD(Map<Resource, Object> brokerCapacity) {
    return brokerCapacity.get(Resource.DISK) instanceof Map;
  }

  @SuppressWarnings("unchecked")
  private static Map<Resource, Double> getTotalCapacity(Map<Resource, Object> brokerCapacity) {
    Map<Resource, Double> totalCapacity = new HashMap<>(brokerCapacity.size());
    if (isJBOD(brokerCapacity)) {
      for (Map.Entry<Resource, Object> entry : brokerCapacity.entrySet()) {
        Resource resource = entry.getKey();
        if (resource == Resource.DISK) {
          double totalDiskCapacity = 0.0;
          for (Map.Entry<String, String> diskEntry : ((Map<String, String>) brokerCapacity.get(resource)).entrySet()) {
            if (!Paths.get(diskEntry.getKey()).isAbsolute()) {
              throw new IllegalArgumentException("The logDir " + diskEntry.getKey() + " must be an absolute path.");
            }

            totalDiskCapacity += Double.parseDouble(diskEntry.getValue());
          }
          totalCapacity.put(resource, totalDiskCapacity);
        } else {
          totalCapacity.put(resource, Double.parseDouble((String) entry.getValue()));
        }
      }
    } else {
      brokerCapacity.forEach((key, value) -> totalCapacity.put(key, Double.parseDouble((String) value)));
    }

    return totalCapacity;
  }

  /**
   * Get disk capacity by absolute logDir path if the capacity is specified per logDir, null otherwise.
   *
   * @param brokerCapacity Broker capacity for each resource.
   * @return Disk capacity by absolute logDir path if the capacity is specified per logDir, null otherwise.
   */
  @SuppressWarnings("unchecked")
  private static Map<String, Double> getDiskCapacityByLogDir(Map<Resource, Object> brokerCapacity) {
    if (!isJBOD(brokerCapacity)) {
      return null;
    }

    Map<String, String> stringDiskCapacityByLogDir = (Map<String, String>) brokerCapacity.get(Resource.DISK);
    Map<String, Double> diskCapacityByLogDir = new HashMap<>(stringDiskCapacityByLogDir.size());
    stringDiskCapacityByLogDir.forEach((key, value) -> diskCapacityByLogDir.put(key, Double.parseDouble(value)));

    return diskCapacityByLogDir;
  }

  private void loadCapacities(String capacityConfigFile) throws FileNotFoundException {
    JsonReader reader = null;
    try {
      reader = new JsonReader(new InputStreamReader(new FileInputStream(capacityConfigFile), StandardCharsets.UTF_8));
      Gson gson = new Gson();
      Set<BrokerCapacity> brokerCapacities = ((BrokerCapacities) gson.fromJson(reader, BrokerCapacities.class)).brokerCapacities;
      _capacitiesForBrokers = new HashMap<>(brokerCapacities.size());
      for (BrokerCapacity bc : brokerCapacities) {
        boolean isDefault = bc.brokerId == DEFAULT_CAPACITY_BROKER_ID;
        Map<Resource, Double> totalCapacity = getTotalCapacity(bc.capacity);
        Map<String, Double> diskCapacityByLogDir = getDiskCapacityByLogDir(bc.capacity);

        BrokerCapacityInfo brokerCapacityInfo =
            isDefault ? new BrokerCapacityInfo(totalCapacity, "The default broker capacity.", diskCapacityByLogDir)
                      : new BrokerCapacityInfo(totalCapacity, diskCapacityByLogDir);
        _capacitiesForBrokers.put(bc.brokerId, brokerCapacityInfo);
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
