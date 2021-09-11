/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.cruisecontrol.common.config;

import com.linkedin.cruisecontrol.common.config.types.Password;
import com.linkedin.cruisecontrol.common.utils.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;


/**
 * This class is used for specifying the set of expected configurations. For each configuration, you can specify
 * the name, the type, the default value, the documentation, the group information, the order in the group,
 * the width of the configuration value and the name suitable for display in the UI.
 *
 * You can provide special validation logic used for single configuration validation by overriding {@link Validator}.
 *
 * Moreover, you can specify the dependents of a configuration. The valid values and visibility of a configuration
 * may change according to the values of other configurations. You can override {@link Recommender} to get valid
 * values and set visibility of a configuration given the current configuration values.
 *
 * To use the class:
 * <pre>
 * ConfigDef defs = new ConfigDef();
 *
 * defs.define(&quot;config_with_default&quot;, Type.STRING, &quot;default string value&quot;, &quot;Configuration with default value.&quot;);
 * defs.define(&quot;config_with_validator&quot;, Type.INT, 42, Range.atLeast(0), &quot;Configuration with user provided validator.&quot;);
 * defs.define(&quot;config_with_dependents&quot;, Type.INT, &quot;Configuration with dependents.&quot;, &quot;group&quot;,
 * 1, &quot;Config With Dependents&quot;, Arrays.asList(&quot;config_with_default&quot;,&quot;config_with_validator&quot;));
 *
 * Map&lt;String, String&gt; props = new HashMap&lt;&gt;();
 * props.put(&quot;config_with_default&quot;, &quot;some value&quot;);
 * props.put(&quot;config_with_dependents&quot;, &quot;some other value&quot;);
 *
 * Map&lt;String, Object&gt; configs = defs.parse(props);
 * // will return &quot;some value&quot;
 * String someConfig = (String) configs.get(&quot;config_with_default&quot;);
 * // will return default value of 42
 * int anotherConfig = (Integer) configs.get(&quot;config_with_validator&quot;);
 *
 * To validate the full configuration, use:
 * List&lt;Config&gt; configs = defs.validate(props);
 * The {@link Config} contains updated configuration information given the current configuration values.
 * </pre>
 * This class can be used standalone or in combination with {@link AbstractConfig} which provides some additional
 * functionality for accessing configs.
 */
public class ConfigDef {
  /**
   * A unique Java object which represents the lack of a default value.
   */
  public static final Object NO_DEFAULT_VALUE = new Object();

  private final Map<String, ConfigKey> _configKeys;
  private final List<String> _groups;
  private Set<String> _configsWithNoParent;

  public ConfigDef() {
    _configKeys = new LinkedHashMap<>();
    _groups = new LinkedList<>();
    _configsWithNoParent = null;
  }

  public ConfigDef(ConfigDef base) {
    _configKeys = new LinkedHashMap<>(base._configKeys);
    _groups = new LinkedList<>(base._groups);
    // It is not safe to copy this from the parent because we may subsequently add to the set of configs and
    // invalidate this
    _configsWithNoParent = null;
  }

  /**
   * Returns unmodifiable set of properties names defined in this {@linkplain ConfigDef}
   *
   * @return New unmodifiable {@link Set} instance containing the keys
   */
  public Set<String> names() {
    return Collections.unmodifiableSet(_configKeys.keySet());
  }

  /**
   * Define the given config key and return this config definition.
   *
   * @param key Config key
   * @return This config definition.
   */
  public ConfigDef define(ConfigKey key) {
    if (_configKeys.containsKey(key._name)) {
      throw new ConfigException("Configuration " + key._name + " is defined twice.");
    }
    if (key._group != null && !_groups.contains(key._group)) {
      _groups.add(key._group);
    }
    _configKeys.put(key._name, key);
    return this;
  }

  /**
   * Define a new configuration
   * @param name          the name of the config parameter
   * @param type          the type of the config
   * @param defaultValue  the default value to use if this config isn't present
   * @param validator     the validator to use in checking the correctness of the config
   * @param importance    the importance of this config
   * @param documentation the documentation string for the config
   * @param group         the group this config belongs to
   * @param orderInGroup  the order of this config in the group
   * @param width         the width of the config
   * @param displayName   the name suitable for display
   * @param dependents    the configurations that are dependents of this configuration
   * @param recommender   the recommender provides valid values given the parent configuration values
   * @return This ConfigDef so you can chain calls
   */
  public ConfigDef define(String name, Type type, Object defaultValue, Validator validator, Importance importance,
                          String documentation, String group, int orderInGroup, Width width, String displayName,
                          List<String> dependents, Recommender recommender) {
    return define(
        new ConfigKey(name, type, defaultValue, validator, importance, documentation, group, orderInGroup, width,
                      displayName, dependents, recommender, false));
  }

  /**
   * Define a new configuration with no custom recommender
   * @param name          the name of the config parameter
   * @param type          the type of the config
   * @param defaultValue  the default value to use if this config isn't present
   * @param validator     the validator to use in checking the correctness of the config
   * @param importance    the importance of this config
   * @param documentation the documentation string for the config
   * @param group         the group this config belongs to
   * @param orderInGroup  the order of this config in the group
   * @param width         the width of the config
   * @param displayName   the name suitable for display
   * @param dependents    the configurations that are dependents of this configuration
   * @return This ConfigDef so you can chain calls
   */
  public ConfigDef define(String name, Type type, Object defaultValue, Validator validator, Importance importance,
                          String documentation, String group, int orderInGroup, Width width, String displayName,
                          List<String> dependents) {
    return define(name, type, defaultValue, validator, importance, documentation, group, orderInGroup, width,
                  displayName, dependents, null);
  }

  /**
   * Define a new configuration with no dependents
   * @param name          the name of the config parameter
   * @param type          the type of the config
   * @param defaultValue  the default value to use if this config isn't present
   * @param validator     the validator to use in checking the correctness of the config
   * @param importance    the importance of this config
   * @param documentation the documentation string for the config
   * @param group         the group this config belongs to
   * @param orderInGroup  the order of this config in the group
   * @param width         the width of the config
   * @param displayName   the name suitable for display
   * @param recommender   the recommender provides valid values given the parent configuration values
   * @return This ConfigDef so you can chain calls
   */
  public ConfigDef define(String name, Type type, Object defaultValue, Validator validator, Importance importance,
                          String documentation, String group, int orderInGroup, Width width, String displayName,
                          Recommender recommender) {
    return define(name, type, defaultValue, validator, importance, documentation, group, orderInGroup, width,
                  displayName, Collections.emptyList(), recommender);
  }

  /**
   * Define a new configuration with no dependents and no custom recommender
   * @param name          the name of the config parameter
   * @param type          the type of the config
   * @param defaultValue  the default value to use if this config isn't present
   * @param validator     the validator to use in checking the correctness of the config
   * @param importance    the importance of this config
   * @param documentation the documentation string for the config
   * @param group         the group this config belongs to
   * @param orderInGroup  the order of this config in the group
   * @param width         the width of the config
   * @param displayName   the name suitable for display
   * @return This ConfigDef so you can chain calls
   */
  public ConfigDef define(String name, Type type, Object defaultValue, Validator validator, Importance importance,
                          String documentation, String group, int orderInGroup, Width width, String displayName) {
    return define(name, type, defaultValue, validator, importance, documentation, group, orderInGroup, width,
                  displayName, Collections.emptyList());
  }

  /**
   * Define a new configuration with no special validation logic
   * @param name          the name of the config parameter
   * @param type          the type of the config
   * @param defaultValue  the default value to use if this config isn't present
   * @param importance    the importance of this config
   * @param documentation the documentation string for the config
   * @param group         the group this config belongs to
   * @param orderInGroup  the order of this config in the group
   * @param width         the width of the config
   * @param displayName   the name suitable for display
   * @param dependents    the configurations that are dependents of this configuration
   * @param recommender   the recommender provides valid values given the parent configuration values
   * @return This ConfigDef so you can chain calls
   */
  public ConfigDef define(String name, Type type, Object defaultValue, Importance importance, String documentation,
                          String group, int orderInGroup, Width width, String displayName, List<String> dependents,
                          Recommender recommender) {
    return define(name, type, defaultValue, null, importance, documentation, group, orderInGroup, width, displayName,
                  dependents, recommender);
  }

  /**
   * Define a new configuration with no special validation logic and no custom recommender
   * @param name          the name of the config parameter
   * @param type          the type of the config
   * @param defaultValue  the default value to use if this config isn't present
   * @param importance    the importance of this config
   * @param documentation the documentation string for the config
   * @param group         the group this config belongs to
   * @param orderInGroup  the order of this config in the group
   * @param width         the width of the config
   * @param displayName   the name suitable for display
   * @param dependents    the configurations that are dependents of this configuration
   * @return This ConfigDef so you can chain calls
   */
  public ConfigDef define(String name, Type type, Object defaultValue, Importance importance, String documentation,
                          String group, int orderInGroup, Width width, String displayName, List<String> dependents) {
    return define(name, type, defaultValue, null, importance, documentation, group, orderInGroup, width, displayName,
                  dependents, null);
  }

  /**
   * Define a new configuration with no special validation logic and no custom recommender
   * @param name          the name of the config parameter
   * @param type          the type of the config
   * @param defaultValue  the default value to use if this config isn't present
   * @param importance    the importance of this config
   * @param documentation the documentation string for the config
   * @param group         the group this config belongs to
   * @param orderInGroup  the order of this config in the group
   * @param width         the width of the config
   * @param displayName   the name suitable for display
   * @param recommender   the recommender provides valid values given the parent configuration values
   * @return This ConfigDef so you can chain calls
   */
  public ConfigDef define(String name, Type type, Object defaultValue, Importance importance, String documentation,
                          String group, int orderInGroup, Width width, String displayName, Recommender recommender) {
    return define(name, type, defaultValue, null, importance, documentation, group, orderInGroup, width, displayName,
                  Collections.emptyList(), recommender);
  }

  /**
   * Define a new configuration with no special validation logic, not dependents and no custom recommender
   * @param name          the name of the config parameter
   * @param type          the type of the config
   * @param defaultValue  the default value to use if this config isn't present
   * @param importance    the importance of this config
   * @param documentation the documentation string for the config
   * @param group         the group this config belongs to
   * @param orderInGroup  the order of this config in the group
   * @param width         the width of the config
   * @param displayName   the name suitable for display
   * @return This ConfigDef so you can chain calls
   */
  public ConfigDef define(String name, Type type, Object defaultValue, Importance importance, String documentation,
                          String group, int orderInGroup, Width width, String displayName) {
    return define(name, type, defaultValue, null, importance, documentation, group, orderInGroup, width, displayName,
                  Collections.emptyList());
  }

  /**
   * Define a new configuration with no default value and no special validation logic
   * @param name          the name of the config parameter
   * @param type          the type of the config
   * @param importance    the importance of this config
   * @param documentation the documentation string for the config
   * @param group         the group this config belongs to
   * @param orderInGroup  the order of this config in the group
   * @param width         the width of the config
   * @param displayName   the name suitable for display
   * @param dependents    the configurations that are dependents of this configuration
   * @param recommender   the recommender provides valid values given the parent configuration value
   * @return This ConfigDef so you can chain calls
   */
  public ConfigDef define(String name, Type type, Importance importance, String documentation, String group, int orderInGroup,
                          Width width, String displayName, List<String> dependents, Recommender recommender) {
    return define(name, type, NO_DEFAULT_VALUE, null, importance, documentation, group, orderInGroup, width,
                  displayName, dependents, recommender);
  }

  /**
   * Define a new configuration with no default value, no special validation logic and no custom recommender
   * @param name          the name of the config parameter
   * @param type          the type of the config
   * @param importance    the importance of this config
   * @param documentation the documentation string for the config
   * @param group         the group this config belongs to
   * @param orderInGroup  the order of this config in the group
   * @param width         the width of the config
   * @param displayName   the name suitable for display
   * @param dependents    the configurations that are dependents of this configuration
   * @return This ConfigDef so you can chain calls
   */
  public ConfigDef define(String name, Type type, Importance importance, String documentation, String group, int orderInGroup,
                          Width width, String displayName, List<String> dependents) {
    return define(name, type, NO_DEFAULT_VALUE, null, importance, documentation, group, orderInGroup, width,
                  displayName, dependents, null);
  }

  /**
   * Define a new configuration with no default value, no special validation logic and no custom recommender
   * @param name          the name of the config parameter
   * @param type          the type of the config
   * @param importance    the importance of this config
   * @param documentation the documentation string for the config
   * @param group         the group this config belongs to
   * @param orderInGroup  the order of this config in the group
   * @param width         the width of the config
   * @param displayName   the name suitable for display
   * @param recommender   the recommender provides valid values given the parent configuration value
   * @return This ConfigDef so you can chain calls
   */
  public ConfigDef define(String name, Type type, Importance importance, String documentation, String group, int orderInGroup,
                          Width width, String displayName, Recommender recommender) {
    return define(name, type, NO_DEFAULT_VALUE, null, importance, documentation, group, orderInGroup, width,
                  displayName, Collections.emptyList(), recommender);
  }

  /**
   * Define a new configuration with no default value, no special validation logic, no dependents and no custom recommender
   * @param name          the name of the config parameter
   * @param type          the type of the config
   * @param importance    the importance of this config
   * @param documentation the documentation string for the config
   * @param group         the group this config belongs to
   * @param orderInGroup  the order of this config in the group
   * @param width         the width of the config
   * @param displayName   the name suitable for display
   * @return This ConfigDef so you can chain calls
   */
  public ConfigDef define(String name, Type type, Importance importance, String documentation, String group, int orderInGroup,
                          Width width, String displayName) {
    return define(name, type, NO_DEFAULT_VALUE, null, importance, documentation, group, orderInGroup, width,
                  displayName, Collections.emptyList());
  }

  /**
   * Define a new configuration with no group, no order in group, no width, no display name, no dependents and no custom recommender
   * @param name          the name of the config parameter
   * @param type          the type of the config
   * @param defaultValue  the default value to use if this config isn't present
   * @param validator     the validator to use in checking the correctness of the config
   * @param importance    the importance of this config
   * @param documentation the documentation string for the config
   * @return This ConfigDef so you can chain calls
   */
  public ConfigDef define(String name, Type type, Object defaultValue, Validator validator, Importance importance,
                          String documentation) {
    return define(name, type, defaultValue, validator, importance, documentation, null, -1, Width.NONE, name);
  }

  /**
   * Define a new configuration with no special validation logic
   * @param name          The name of the config parameter
   * @param type          The type of the config
   * @param defaultValue  The default value to use if this config isn't present
   * @param importance    The importance of this config: is this something you will likely need to change.
   * @param documentation The documentation string for the config
   * @return This ConfigDef so you can chain calls
   */
  public ConfigDef define(String name, Type type, Object defaultValue, Importance importance, String documentation) {
    return define(name, type, defaultValue, null, importance, documentation);
  }

  /**
   * Define a new configuration with no default value and no special validation logic
   * @param name          The name of the config parameter
   * @param type          The type of the config
   * @param importance    The importance of this config: is this something you will likely need to change.
   * @param documentation The documentation string for the config
   * @return This ConfigDef so you can chain calls
   */
  public ConfigDef define(String name, Type type, Importance importance, String documentation) {
    return define(name, type, NO_DEFAULT_VALUE, null, importance, documentation);
  }

  /**
   * Define a new internal configuration. Internal configuration won't show up in the docs and aren't
   * intended for general use.
   * @param name              The name of the config parameter
   * @param type              The type of the config
   * @param defaultValue      The default value to use if this config isn't present
   * @param importance        The importance of this config
   * @return This ConfigDef so you can chain calls
   */
  public ConfigDef defineInternal(final String name, final Type type, final Object defaultValue, final Importance importance) {
    return define(new ConfigKey(name, type, defaultValue, null, importance, "", "", -1,
                                Width.NONE, name, Collections.emptyList(), null, true));
  }

  /**
   * Get the configuration keys
   * @return A map containing all configuration keys
   */
  public Map<String, ConfigKey> configKeys() {
    return Collections.unmodifiableMap(_configKeys);
  }

  /**
   * Get the groups for the configuration
   * @return A list of group names
   */
  public List<String> groups() {
    return Collections.unmodifiableList(_groups);
  }

  /**
   * Parse and validate configs against this configuration definition. The input is a map of configs. It is expected
   * that the keys of the map are strings, but the values can either be strings or they may already be of the
   * appropriate type (int, string, etc). This will work equally well with either java.util.Properties instances or a
   * programmatically constructed map.
   *
   * @param props The configs to parse and validate.
   * @return Parsed and validated configs. The key will be the config name and the value will be the value parsed into
   * the appropriate type (int, string, etc).
   */
  public Map<String, Object> parse(Map<?, ?> props) {
    // Check all configurations are defined
    List<String> undefinedConfigKeys = undefinedDependentConfigs();
    if (!undefinedConfigKeys.isEmpty()) {
      String joined = Utils.join(undefinedConfigKeys, ",");
      throw new ConfigException("Some configurations in are referred in the dependents, but not defined: " + joined);
    }
    // parse all known keys
    Map<String, Object> values = new HashMap<>();
    for (ConfigKey key : _configKeys.values()) {
      values.put(key._name, parseValue(key, props.get(key._name), props.containsKey(key._name)));
    }
    return values;
  }

  Object parseValue(ConfigKey key, Object value, boolean isSet) {
    Object parsedValue;
    if (isSet) {
      parsedValue = parseType(key._name, value, key._type);
      // props map doesn't contain setting, the key is required because no default value specified - its an error
    } else if (NO_DEFAULT_VALUE.equals(key._defaultValue)) {
      throw new ConfigException("Missing required configuration \"" + key._name + "\" which has no default value.");
    } else {
      // otherwise assign setting its default value
      parsedValue = key._defaultValue;
    }
    if (key._validator != null) {
      key._validator.ensureValid(key._name, parsedValue);
    }
    return parsedValue;
  }

  /**
   * Validate the current configuration values with the configuration definition.
   * @param props the current configuration values
   * @return List of Config, each Config contains the updated configuration information given
   * the current configuration values.
   */
  public List<ConfigValue> validate(Map<String, String> props) {
    return new ArrayList<>(validateAll(props).values());
  }

  private Map<String, ConfigValue> validate(Map<String, Object> parsed, Map<String, ConfigValue> configValues) {
    Set<String> configsWithNoParent = getConfigsWithNoParent();
    for (String name : configsWithNoParent) {
      validate(name, parsed, configValues);
    }
    return configValues;
  }

  private void validate(String name, Map<String, Object> parsed, Map<String, ConfigValue> configs) {
    if (!_configKeys.containsKey(name)) {
      return;
    }

    ConfigKey key = _configKeys.get(name);
    ConfigValue value = configs.get(name);

    if (key._recommender != null) {
      try {
        List<Object> recommendedValues = key._recommender.validValues(name, parsed);
        List<Object> originalRecommendedValues = value.recommendedValues();
        if (!originalRecommendedValues.isEmpty()) {
          Set<Object> originalRecommendedValueSet = new HashSet<>(originalRecommendedValues);
          recommendedValues.removeIf(o -> !originalRecommendedValueSet.contains(o));
        }
        value.recommendedValues(recommendedValues);
        value.visible(key._recommender.visible(name, parsed));
      } catch (ConfigException e) {
        value.addErrorMessage(e.getMessage());
      }
    }

    configs.put(name, value);
    for (String dependent : key._dependents) {
      validate(dependent, parsed, configs);
    }
  }

  /**
   * Validate all configuration values.
   *
   * @param props The configuration values.
   * @return A map of validated config values.
   */
  public Map<String, ConfigValue> validateAll(Map<String, String> props) {
    Map<String, ConfigValue> configValues = new HashMap<>();
    for (String name : _configKeys.keySet()) {
      configValues.put(name, new ConfigValue(name));
    }

    List<String> undefinedConfigKeys = undefinedDependentConfigs();
    for (String undefinedConfigKey : undefinedConfigKeys) {
      ConfigValue undefinedConfigValue = new ConfigValue(undefinedConfigKey);
      undefinedConfigValue.addErrorMessage(undefinedConfigKey + " is referred in the dependents, but not defined.");
      undefinedConfigValue.visible(false);
      configValues.put(undefinedConfigKey, undefinedConfigValue);
    }

    Map<String, Object> parsed = parseForValidate(props, configValues);
    return validate(parsed, configValues);
  }

  // package accessible for testing
  Map<String, Object> parseForValidate(Map<String, String> props, Map<String, ConfigValue> configValues) {
    Map<String, Object> parsed = new HashMap<>();
    Set<String> configsWithNoParent = getConfigsWithNoParent();
    for (String name : configsWithNoParent) {
      parseForValidate(name, props, parsed, configValues);
    }
    return parsed;
  }

  private void parseForValidate(String name, Map<String, String> props, Map<String, Object> parsed, Map<String, ConfigValue> configs) {
    if (!_configKeys.containsKey(name)) {
      return;
    }
    ConfigKey key = _configKeys.get(name);
    ConfigValue config = configs.get(name);

    Object value = null;
    if (props.containsKey(key._name)) {
      try {
        value = parseType(key._name, props.get(key._name), key._type);
      } catch (ConfigException e) {
        config.addErrorMessage(e.getMessage());
      }
    } else if (NO_DEFAULT_VALUE.equals(key._defaultValue)) {
      config.addErrorMessage("Missing required configuration \"" + key._name + "\" which has no default value.");
    } else {
      value = key._defaultValue;
    }

    if (key._validator != null) {
      try {
        key._validator.ensureValid(key._name, value);
      } catch (ConfigException e) {
        config.addErrorMessage(e.getMessage());
      }
    }
    config.value(value);
    parsed.put(name, value);
    for (String dependent : key._dependents) {
      parseForValidate(dependent, props, parsed, configs);
    }
  }

  private List<String> undefinedDependentConfigs() {
    Set<String> undefinedConfigKeys = new HashSet<>();
    for (ConfigKey configKey : _configKeys.values()) {
      for (String dependent : configKey._dependents) {
        if (!_configKeys.containsKey(dependent)) {
          undefinedConfigKeys.add(dependent);
        }
      }
    }
    return new ArrayList<>(undefinedConfigKeys);
  }

  // package accessible for testing
  Set<String> getConfigsWithNoParent() {
    if (this._configsWithNoParent != null) {
      return this._configsWithNoParent;
    }
    Set<String> configsWithParent = new HashSet<>();

    for (ConfigKey configKey : _configKeys.values()) {
      List<String> dependents = configKey._dependents;
      configsWithParent.addAll(dependents);
    }

    Set<String> configs = new HashSet<>(_configKeys.keySet());
    configs.removeAll(configsWithParent);
    this._configsWithNoParent = configs;
    return configs;
  }

  /**
   * Parse a value according to its expected type.
   * @param name  The config name
   * @param value The config value
   * @param type  The expected type
   * @return The parsed object
   */
  public static Object parseType(String name, Object value, Type type) {
    try {
      if (value == null) {
        return null;
      }

      String trimmed = null;
      if (value instanceof String) {
        trimmed = ((String) value).trim();
      }

      switch (type) {
        case BOOLEAN:
          if (value instanceof String) {
            if ("true".equalsIgnoreCase(trimmed)) {
              return true;
            } else if ("false".equalsIgnoreCase(trimmed)) {
              return false;
            } else {
              throw new ConfigException(name, value, "Expected value to be either true or false");
            }
          } else if (value instanceof Boolean) {
            return value;
          } else {
            throw new ConfigException(name, value, "Expected value to be either true or false");
          }
        case PASSWORD:
          if (value instanceof Password) {
            return value;
          } else if (value instanceof String) {
            return new Password(trimmed);
          } else {
            throw new ConfigException(name, value, "Expected value to be a string, but it was a " + value.getClass().getName());
          }
        case STRING:
          if (value instanceof String) {
            return trimmed;
          } else {
            throw new ConfigException(name, value, "Expected value to be a string, but it was a " + value.getClass().getName());
          }
        case INT:
          if (value instanceof Integer) {
            return value;
          } else if (value instanceof String) {
            return Integer.parseInt(trimmed);
          } else {
            throw new ConfigException(name, value,
                                      "Expected value to be a 32-bit integer, but it was a " + value.getClass().getName());
          }
        case SHORT:
          if (value instanceof Short) {
            return value;
          } else if (value instanceof String) {
            return Short.parseShort(trimmed);
          } else {
            throw new ConfigException(name, value,
                                      "Expected value to be a 16-bit integer (short), but it was a " + value.getClass().getName());
          }
        case LONG:
          if (value instanceof Integer) {
            return ((Integer) value).longValue();
          }
          if (value instanceof Long) {
            return value;
          } else if (value instanceof String) {
            return Long.parseLong(trimmed);
          } else {
            throw new ConfigException(name, value, "Expected value to be a 64-bit integer (long), but it was a " + value.getClass().getName());
          }
        case DOUBLE:
          if (value instanceof Number) {
            return ((Number) value).doubleValue();
          } else if (value instanceof String) {
            return Double.parseDouble(trimmed);
          } else {
            throw new ConfigException(name, value, "Expected value to be a double, but it was a " + value.getClass().getName());
          }
        case LIST:
          if (value instanceof List) {
            return value;
          } else if (value instanceof String) {
            if (trimmed.isEmpty()) {
              return Collections.emptyList();
            } else {
              return Arrays.asList(trimmed.split("\\s*,\\s*", -1));
            }
          } else {
            throw new ConfigException(name, value, "Expected a comma separated list.");
          }
        case CLASS:
          if (value instanceof Class) {
            return value;
          } else if (value instanceof String) {
            return Class.forName(trimmed, true, Utils.getContextOrCruiseControlClassLoader());
          } else {
            throw new ConfigException(name, value, "Expected a Class instance or class name.");
          }
        default:
          throw new IllegalStateException("Unknown type.");
      }
    } catch (NumberFormatException e) {
      throw new ConfigException(name, value, "Not a number of type " + type);
    } catch (ClassNotFoundException e) {
      throw new ConfigException(name, value, "Class " + value + " could not be found.");
    }
  }

  /**
   * Convert the parsed value with the given type to String.
   *
   * @param parsedValue Parsed value.
   * @param type Type.
   * @return The parsed value with the given type as String.
   */
  public static String convertToString(Object parsedValue, Type type) {
    if (parsedValue == null) {
      return null;
    }

    if (type == null) {
      return parsedValue.toString();
    }

    switch (type) {
      case BOOLEAN:
      case SHORT:
      case INT:
      case LONG:
      case DOUBLE:
      case STRING:
      case PASSWORD:
        return parsedValue.toString();
      case LIST:
        List<?> valueList = (List<?>) parsedValue;
        return Utils.join(valueList, ",");
      case CLASS:
        Class<?> clazz = (Class<?>) parsedValue;
        return clazz.getName();
      default:
        throw new IllegalStateException("Unknown type.");
    }
  }

  /**
   * The config types
   */
  public enum Type {
    BOOLEAN, STRING, INT, SHORT, LONG, DOUBLE, LIST, CLASS, PASSWORD
  }

  /**
   * The importance level for a configuration
   */
  public enum Importance {
    HIGH, MEDIUM, LOW
  }

  /**
   * The width of a configuration value
   */
  public enum Width {
    NONE, SHORT, MEDIUM, LONG
  }

  /**
   * This is used by the {@link #validate(Map)} to get valid values for a configuration given the current
   * configuration values in order to perform full configuration validation and visibility modification.
   * In case that there are dependencies between configurations, the valid values and visibility
   * for a configuration may change given the values of other configurations.
   */
  public interface Recommender {

    /**
     * The valid values for the configuration given the current configuration values.
     * @param name The name of the configuration
     * @param parsedConfig The parsed configuration values
     * @return The list of valid values. To function properly, the returned objects should have the type
     * defined for the configuration using the recommender.
     */
    List<Object> validValues(String name, Map<String, Object> parsedConfig);

    /**
     * Set the visibility of the configuration given the current configuration values.
     * @param name The name of the configuration
     * @param parsedConfig The parsed configuration values
     * @return The visibility of the configuration
     */
    boolean visible(String name, Map<String, Object> parsedConfig);
  }

  /**
   * Validation logic the user may provide to perform single configuration validation.
   */
  public interface Validator {
    /**
     * Perform single configuration validation.
     * @param name The name of the configuration
     * @param value The value of the configuration
     * @throws ConfigException if the value is invalid.
     */
    void ensureValid(String name, Object value);
  }

  /**
   * Validation logic for numeric ranges
   */
  public static final class Range implements Validator {
    private final Number _min;
    private final Number _max;

    private Range(Number min, Number max) {
      this._min = min;
      this._max = max;
    }

    /**
     * A numeric range that checks only the lower bound
     *
     * @param min The minimum acceptable value
     * @return A numeric range that checks only the lower bound.
     */
    public static Range atLeast(Number min) {
      return new Range(min, null);
    }

    /**
     * @param min Minimum bound.
     * @param max Maximum bound.
     * @return A numeric range that checks both the upper and lower bound
     */
    public static Range between(Number min, Number max) {
      return new Range(min, max);
    }

    /**
     * Object to ensure validity for the given config name.
     *
     * @param name The name of the configuration
     * @param o Object to ensure validity.
     */
    public void ensureValid(String name, Object o) {
      if (o == null) {
        throw new ConfigException(name, null, "Value must be non-null");
      }
      Number n = (Number) o;
      if (_min != null && n.doubleValue() < _min.doubleValue()) {
        throw new ConfigException(name, o, "Value must be at least " + _min);
      }
      if (_max != null && n.doubleValue() > _max.doubleValue()) {
        throw new ConfigException(name, o, "Value must be no more than " + _max);
      }
    }

    /**
     * @return String representation of the numeric range.
     */
    public String toString() {
      if (_min == null) {
        return "[...," + _max + "]";
      } else if (_max == null) {
        return "[" + _min + ",...]";
      } else {
        return "[" + _min + ",...," + _max + "]";
      }
    }
  }

  public static final class ValidList implements Validator {

    protected final ValidString _validString;

    private ValidList(List<String> validStrings) {
      this._validString = new ValidString(validStrings);
    }

    public static ValidList in(String... validStrings) {
      return new ValidList(Arrays.asList(validStrings));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void ensureValid(final String name, final Object value) {
      List<String> values = (List<String>) value;
      for (String string : values) {
        _validString.ensureValid(name, string);
      }
    }

    public String toString() {
      return _validString.toString();
    }
  }

  public static final class ValidString implements Validator {
    protected final List<String> _validStrings;

    private ValidString(List<String> validStrings) {
      this._validStrings = validStrings;
    }

    public static ValidString in(String... validStrings) {
      return new ValidString(Arrays.asList(validStrings));
    }

    @Override
    public void ensureValid(String name, Object o) {
      String s = (String) o;
      if (!_validStrings.contains(s)) {
        throw new ConfigException(name, o, "String must be one of: " + Utils.join(_validStrings, ", "));
      }
    }

    public String toString() {
      return "[" + Utils.join(_validStrings, ", ") + "]";
    }
  }

  public static class NonEmptyString implements Validator {

    @Override
    public void ensureValid(String name, Object o) {
      String s = (String) o;
      if (s != null && s.isEmpty()) {
        throw new ConfigException(name, o, "String must be non-empty");
      }
    }

    @Override
    public String toString() {
      return "non-empty string";
    }
  }

  public static class ConfigKey {
    protected final String _name;
    protected final Type _type;
    protected final String _documentation;
    protected final Object _defaultValue;
    protected final Validator _validator;
    protected final Importance _importance;
    protected final String _group;
    protected final int _orderInGroup;
    protected final Width _width;
    protected final String _displayName;
    protected final List<String> _dependents;
    protected final Recommender _recommender;
    protected final boolean _internalConfig;

    public ConfigKey(String name, Type type, Object defaultValue, Validator validator, Importance importance, String documentation, String group,
                     int orderInGroup, Width width, String displayName, List<String> dependents, Recommender recommender, boolean internalConfig) {
      _name = name;
      _type = type;
      _defaultValue = NO_DEFAULT_VALUE.equals(defaultValue) ? NO_DEFAULT_VALUE : parseType(name, defaultValue, type);
      _validator = validator;
      _importance = importance;
      if (_validator != null && hasDefault()) {
        _validator.ensureValid(name, _defaultValue);
      }
      _documentation = documentation;
      _dependents = dependents;
      _group = group;
      _orderInGroup = orderInGroup;
      _width = width;
      _displayName = displayName;
      _recommender = recommender;
      _internalConfig = internalConfig;
    }

    public boolean hasDefault() {
      return !NO_DEFAULT_VALUE.equals(this._defaultValue);
    }

    public String name() {
      return _name;
    }

    public Type type() {
      return _type;
    }

    public String documentation() {
      return _documentation;
    }

    public Object defaultValue() {
      return _defaultValue;
    }

    public Validator validator() {
      return _validator;
    }

    public Importance importance() {
      return _importance;
    }

    public String group() {
      return _group;
    }

    public int orderInGroup() {
      return _orderInGroup;
    }

    public Width width() {
      return _width;
    }

    public String displayName() {
      return _displayName;
    }

    public List<String> dependents() {
      return Collections.unmodifiableList(_dependents);
    }

    public Recommender recommender() {
      return _recommender;
    }

    public boolean internalConfig() {
      return _internalConfig;
    }
  }

  protected List<String> headers() {
    return Arrays.asList("Name", "Description", "Type", "Default", "Valid Values", "Importance");
  }

  protected String getConfigValue(ConfigKey key, String headerName) {
    switch (headerName) {
      case "Name":
        return key._name;
      case "Description":
        return key._documentation;
      case "Type":
        return key._type.toString().toLowerCase(Locale.ROOT);
      case "Default":
        if (key.hasDefault()) {
          if (key._defaultValue == null) {
            return "null";
          }
          String defaultValueStr = convertToString(key._defaultValue, key._type);
          if (defaultValueStr.isEmpty()) {
            return "\"\"";
          } else {
            return defaultValueStr;
          }
        } else {
          return "";
        }
      case "Valid Values":
        return key._validator != null ? key._validator.toString() : "";
      case "Importance":
        return key._importance.toString().toLowerCase(Locale.ROOT);
      default:
        throw new RuntimeException("Can't find value for header '" + headerName + "' in " + key._name);
    }
  }

  /**
   * @return HTML table.
   */
  public String toHtmlTable() {
    List<ConfigKey> configs = sortedConfigs();
    StringBuilder b = new StringBuilder();
    b.append("<table class=\"data-table\"><tbody>\n");
    b.append("<tr>\n");
    // print column headers
    for (String headerName : headers()) {
      b.append("<th>");
      b.append(headerName);
      b.append("</th>\n");
    }
    b.append("</tr>\n");
    for (ConfigKey key : configs) {
      if (key._internalConfig) {
        continue;
      }
      b.append("<tr>\n");
      // print column values
      for (String headerName : headers()) {
        b.append("<td>");
        b.append(getConfigValue(key, headerName));
        b.append("</td>");
      }
      b.append("</tr>\n");
    }
    b.append("</tbody></table>");
    return b.toString();
  }

  /**
   * @return the configs formatted with reStructuredText, suitable for embedding in Sphinx documentation.
   */
  public String toRst() {
    StringBuilder b = new StringBuilder();
    for (ConfigKey key : sortedConfigs()) {
      if (key._internalConfig) {
        continue;
      }
      getConfigKeyRst(key, b);
      b.append("\n");
    }
    return b.toString();
  }

  /**
   * Configs with new metadata (group, orderInGroup, dependents) formatted with reStructuredText, suitable for embedding in Sphinx
   * documentation.
   * @return Enriched reStructuredText.
   */
  public String toEnrichedRst() {
    StringBuilder b = new StringBuilder();

    String lastKeyGroupName = "";
    for (ConfigKey key : sortedConfigs()) {
      if (key._internalConfig) {
        continue;
      }
      if (key._group != null) {
        if (!lastKeyGroupName.equalsIgnoreCase(key._group)) {
          b.append(key._group).append("\n");

          char[] underLine = new char[key._group.length()];
          Arrays.fill(underLine, '^');
          b.append(new String(underLine)).append("\n\n");
        }
        lastKeyGroupName = key._group;
      }

      getConfigKeyRst(key, b);

      if (key._dependents != null && key._dependents.size() > 0) {
        int j = 0;
        b.append("  * Dependents: ");
        for (String dependent : key._dependents) {
          b.append("``");
          b.append(dependent);
          if (++j == key._dependents.size()) {
            b.append("``");
          } else {
            b.append("``, ");
          }
        }
        b.append("\n");
      }
      b.append("\n");
    }
    return b.toString();
  }

  /**
   * Shared content on Rst and Enriched Rst.
   * @param key Config key.
   * @param b String builder to append relevant information.
   */
  private void getConfigKeyRst(ConfigKey key, StringBuilder b) {
    b.append("``").append(key._name).append("``").append("\n");
    for (String docLine : key._documentation.split("\n")) {
      if (docLine.length() == 0) {
        continue;
      }
      b.append("  ").append(docLine).append("\n\n");
    }
    b.append("  * Type: ").append(getConfigValue(key, "Type")).append("\n");
    if (key.hasDefault()) {
      b.append("  * Default: ").append(getConfigValue(key, "Default")).append("\n");
    }
    if (key._validator != null) {
      b.append("  * Valid Values: ").append(getConfigValue(key, "Valid Values")).append("\n");
    }
    b.append("  * Importance: ").append(getConfigValue(key, "Importance")).append("\n");
  }

  /**
   * Get a list of configs sorted taking the 'group' and 'orderInGroup' into account.
   *
   * If grouping is not specified, the result will reflect "natural" order: listing required fields first,
   * then ordering by importance, and finally by name.
   * @return A list of configs sorted taking the 'group' and 'orderInGroup' into account.
   */
  private List<ConfigKey> sortedConfigs() {
    final Map<String, Integer> groupOrd = new HashMap<>();
    int ord = 0;
    for (String group : _groups) {
      groupOrd.put(group, ord++);
    }

    List<ConfigKey> configs = new ArrayList<>(_configKeys.values());
    configs.sort(new Comparator<ConfigKey>() {
      @Override
      public int compare(ConfigKey k1, ConfigKey k2) {
        int cmp = k1._group == null ? (k2._group == null ? 0 : -1)
            : (k2._group == null ? 1 : Integer.compare(groupOrd.get(k1._group), groupOrd.get(k2._group)));
        if (cmp == 0) {
          cmp = Integer.compare(k1._orderInGroup, k2._orderInGroup);
          if (cmp == 0) {
            // first take anything with no default value
            if (!k1.hasDefault() && k2.hasDefault()) {
              cmp = -1;
            } else if (!k2.hasDefault() && k1.hasDefault()) {
              cmp = 1;
            } else {
              cmp = k1._importance.compareTo(k2._importance);
              if (cmp == 0) {
                return k1._name.compareTo(k2._name);
              }
            }
          }
        }
        return cmp;
      }
    });
    return configs;
  }

  /**
   * For each {@link ConfigKey} in the given child, creates a {@link ConfigKey} and passes it to {@link #define(ConfigKey)}.
   *
   * @param keyPrefix Key prefix.
   * @param groupPrefix Group prefix.
   * @param startingOrd Staring order.
   * @param child Child config definition.
   */
  public void embed(final String keyPrefix, final String groupPrefix, final int startingOrd, final ConfigDef child) {
    int orderInGroup = startingOrd;
    for (ConfigKey key : child.sortedConfigs()) {
      define(new ConfigKey(keyPrefix + key._name, key._type, key._defaultValue, embeddedValidator(keyPrefix, key._validator),
                           key._importance, key._documentation, groupPrefix + (key._group == null ? "" : ": " + key._group),
                           orderInGroup++, key._width, key._displayName, embeddedDependents(keyPrefix, key._dependents),
                           embeddedRecommender(keyPrefix, key._recommender), key._internalConfig));
    }
  }

  /**
   * @param keyPrefix Key prefix.
   * @param base Base validator.
   * @return A new validator instance that delegates to the base validator but unprefixes the config name along the way.
   */
  private static Validator embeddedValidator(final String keyPrefix, final Validator base) {
    if (base == null) {
      return null;
    }
    return new ConfigDef.Validator() {
      @Override
      public void ensureValid(String name, Object value) {
        base.ensureValid(name.substring(keyPrefix.length()), value);
      }
    };
  }

  /**
   * Updated list of dependent configs with the specified {@code prefix} added.
   * @param keyPrefix Key prefix to be added.
   * @param dependents Dependents to be updated.
   * @return Updated dependents.
   */
  private static List<String> embeddedDependents(final String keyPrefix, final List<String> dependents) {
    if (dependents == null) {
      return null;
    }
    final List<String> updatedDependents = new ArrayList<>(dependents.size());
    for (String dependent : dependents) {
      updatedDependents.add(keyPrefix + dependent);
    }
    return updatedDependents;
  }

  /**
   * @param keyPrefix Key prefix.
   * @param base The base recommender.
   * @return A new recommender instance that delegates to the base recommender but unprefixes the input parameters along the way.
   */
  private static Recommender embeddedRecommender(final String keyPrefix, final Recommender base) {
    if (base == null) {
      return null;
    }
    return new Recommender() {
      private String unprefixed(String k) {
        return k.substring(keyPrefix.length());
      }

      private Map<String, Object> unprefixed(Map<String, Object> parsedConfig) {
        final Map<String, Object> unprefixedParsedConfig = new HashMap<>();
        for (Map.Entry<String, Object> e : parsedConfig.entrySet()) {
          if (e.getKey().startsWith(keyPrefix)) {
            unprefixedParsedConfig.put(unprefixed(e.getKey()), e.getValue());
          }
        }
        return unprefixedParsedConfig;
      }

      @Override
      public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
        return base.validValues(unprefixed(name), unprefixed(parsedConfig));
      }

      @Override
      public boolean visible(String name, Map<String, Object> parsedConfig) {
        return base.visible(unprefixed(name), unprefixed(parsedConfig));
      }
    };
  }
}
