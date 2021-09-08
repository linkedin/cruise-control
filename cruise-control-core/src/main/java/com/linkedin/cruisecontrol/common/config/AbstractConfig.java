/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.cruisecontrol.common.config;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import com.linkedin.cruisecontrol.common.config.types.Password;
import com.linkedin.cruisecontrol.common.utils.Utils;
import com.linkedin.cruisecontrol.exception.CruiseControlException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A convenient base class for configurations to extend.
 * <p>
 * This class holds both the original configuration that was provided as well as the parsed
 */
public class AbstractConfig {

  public static final String NL = System.getProperty("line.separator");

  private final Logger _log = LoggerFactory.getLogger(getClass());

  /* configs for which values have been requested, used to detect unused configs */
  private final Set<String> _used;

  /* the original values passed in by the user */
  private final Map<String, ?> _originals;

  /* the parsed values */
  private final Map<String, Object> _values;

  private final ConfigDef _definition;

  @SuppressWarnings("unchecked")
  public AbstractConfig(ConfigDef definition, Map<?, ?> originals, boolean doLog) {
    /* check that all the keys are really strings */
    for (Map.Entry<?, ?> entry : originals.entrySet()) {
      if (!(entry.getKey() instanceof String)) {
        throw new ConfigException(entry.getKey().toString(), entry.getValue(), "Key must be a string.");
      }
    }
    _originals = (Map<String, ?>) originals;
    _values = definition.parse(this._originals);
    Map<String, Object> configUpdates = postProcessParsedConfig(Collections.unmodifiableMap(_values));
    for (Map.Entry<String, Object> update : configUpdates.entrySet()) {
      _values.put(update.getKey(), update.getValue());
    }
    definition.parse(_values);
    _used = Collections.synchronizedSet(new HashSet<>());
    this._definition = definition;
    if (doLog) {
      logAll();
    }
  }

  public AbstractConfig(ConfigDef definition, Map<?, ?> originals) {
    this(definition, originals, true);
  }

  /**
   * Called directly after user configs got parsed (and thus default values got set).
   * This allows to change default values for "secondary defaults" if required.
   *
   * @param parsedValues unmodifiable map of current configuration
   * @return A map of updates that should be applied to the configuration (will be validated to prevent bad updates)
   */
  protected Map<String, Object> postProcessParsedConfig(Map<String, Object> parsedValues) {
    return Collections.emptyMap();
  }

  protected Object get(String key) {
    if (!_values.containsKey(key)) {
      throw new ConfigException(String.format("Unknown configuration '%s'", key));
    }
    _used.add(key);
    return _values.get(key);
  }

  public void ignore(String key) {
    _used.add(key);
  }

  public Short getShort(String key) {
    return (Short) get(key);
  }

  public Integer getInt(String key) {
    return (Integer) get(key);
  }

  public Long getLong(String key) {
    return (Long) get(key);
  }

  public Double getDouble(String key) {
    return (Double) get(key);
  }

  @SuppressWarnings("unchecked")
  public List<String> getList(String key) {
    return (List<String>) get(key);
  }

  public Boolean getBoolean(String key) {
    return (Boolean) get(key);
  }

  public String getString(String key) {
    return (String) get(key);
  }

  /**
   * Get the type of the given key.
   *
   * @param key A config key to retrieve the type.
   * @return The type of the given key.
   */
  public ConfigDef.Type typeOf(String key) {
    ConfigDef.ConfigKey configKey = _definition.configKeys().get(key);
    if (configKey == null) {
      return null;
    }
    return configKey.type();
  }

  public Password getPassword(String key) {
    return (Password) get(key);
  }

  public Class<?> getClass(String key) {
    return (Class<?>) get(key);
  }

  /**
   * @return Unused configs.
   */
  public Set<String> unused() {
    Set<String> keys = new HashSet<>(_originals.keySet());
    keys.removeAll(_used);
    return keys;
  }

  /**
   * @return Original configs.
   */
  public Map<String, Object> originals() {
    Map<String, Object> copy = new RecordingMap<>();
    copy.putAll(_originals);
    return copy;
  }

  /**
   * Get all the original settings, ensuring that all values are of type String.
   * @return The original settings
   * @throws ClassCastException if any of the values are not strings
   */
  public Map<String, String> originalsStrings() {
    Map<String, String> copy = new RecordingMap<>();
    for (Map.Entry<String, ?> entry : _originals.entrySet()) {
      if (!(entry.getValue() instanceof String)) {
        throw new ClassCastException(
            "Non-string value found in original settings for key " + entry.getKey() + ": "
                + (entry.getValue() == null ? null : entry.getValue().getClass().getName()));
      }
      copy.put(entry.getKey(), (String) entry.getValue());
    }
    return copy;
  }

  /**
   * Gets all original settings with the given prefix, stripping the prefix before adding it to the output.
   *
   * @param prefix the prefix to use as a filter
   * @return A Map containing the settings with the prefix
   */
  public Map<String, Object> originalsWithPrefix(String prefix) {
    Map<String, Object> result = new RecordingMap<>(prefix, false);
    for (Map.Entry<String, ?> entry : _originals.entrySet()) {
      if (entry.getKey().startsWith(prefix) && entry.getKey().length() > prefix.length()) {
        result.put(entry.getKey().substring(prefix.length()), entry.getValue());
      }
    }
    return result;
  }

  /**
   * Put all keys that do not start with {@code prefix} and their parsed values in the result map and then
   * put all the remaining keys with the prefix stripped and their parsed values in the result map.
   *
   * This is useful if one wants to allow prefixed configs to override default ones.
   * @param prefix The prefix to use as override.
   * @return Values with prefix override.
   */
  public Map<String, Object> valuesWithPrefixOverride(String prefix) {
    Map<String, Object> result = new RecordingMap<>(values(), prefix, true);
    for (Map.Entry<String, ?> entry : _originals.entrySet()) {
      if (entry.getKey().startsWith(prefix) && entry.getKey().length() > prefix.length()) {
        String keyWithNoPrefix = entry.getKey().substring(prefix.length());
        ConfigDef.ConfigKey configKey = _definition.configKeys().get(keyWithNoPrefix);
        if (configKey != null) {
          result.put(keyWithNoPrefix, _definition.parseValue(configKey, entry.getValue(), true));
        }
      }
    }
    return result;
  }

  public Map<String, ?> values() {
    return new RecordingMap<>(_values);
  }

  private void logAll() {
    StringBuilder b = new StringBuilder();
    b.append(getClass().getSimpleName());
    b.append(" values: ");
    b.append(NL);

    for (Map.Entry<String, Object> entry : new TreeMap<>(_values).entrySet()) {
      b.append('\t');
      b.append(entry.getKey());
      b.append(" = ");
      b.append(entry.getValue());
      b.append(NL);
    }
    _log.info(b.toString());
  }

  /**
   * Log warnings for any unused configurations
   */
  public void logUnused() {
    for (String key : unused()) {
      _log.warn("The configuration '{}' was supplied but isn't a known config.", key);
    }
  }

  /**
   * Get a configured instance of the give class specified by the given configuration key. If the object implements
   * Configurable configure it using the configuration.
   *
   * @param key The configuration key for the class
   * @param t The interface the class should implement
   * @param <T> The type of the configured instance to be returned.
   * @return A configured instance of the class
   */
  public <T> T getConfiguredInstance(String key, Class<T> t) throws CruiseControlException {
    Class<?> c = getClass(key);
    if (c == null) {
      return null;
    }
    Object o = Utils.newInstance(c);
    if (!t.isInstance(o)) {
      throw new CruiseControlException(c.getName() + " is not an instance of " + t.getName());
    }
    if (o instanceof CruiseControlConfigurable) {
      ((CruiseControlConfigurable) o).configure(originals());
    }
    return t.cast(o);
  }

  /**
   * Get a list of configured instances of the given class specified by the given configuration key. The configuration
   * may specify either {@code null} or an empty string to indicate no configured instances. In both cases, this method
   * returns an empty list to indicate no configured instances.
   * @param key The configuration key for the class
   * @param t The interface the class should implement
   * @param <T> The type of the configured instance to be returned.
   * @return The list of configured instances
   */
  @SuppressWarnings("unchecked")
  public <T> List<T> getConfiguredInstances(String key, Class<T> t) throws CruiseControlException {
    return getConfiguredInstances(key, t, Collections.EMPTY_MAP);
  }

  /**
   * Get a list of configured instances of the given class specified by the given configuration key. The configuration
   * may specify either {@code null} or an empty string to indicate no configured instances. In both cases, this method
   * returns an empty list to indicate no configured instances.
   * @param key The configuration key for the class
   * @param t The interface the class should implement
   * @param configOverrides Configuration overrides to use.
   * @param <T> The type of the configured instances to be returned.
   * @return The list of configured instances
   */
  public <T> List<T> getConfiguredInstances(String key, Class<T> t, Map<String, Object> configOverrides)
      throws CruiseControlException {
    List<String> klasses = getList(key);
    List<T> objects = new ArrayList<>();
    if (klasses == null) {
      return objects;
    }
    Map<String, Object> configPairs = originals();
    configPairs.putAll(configOverrides);
    for (Object klass : klasses) {
      Object o;
      if (klass instanceof String) {
        try {
          o = Utils.newInstance((String) klass, t);
        } catch (ClassNotFoundException e) {
          throw new CruiseControlException(klass + " ClassNotFoundException exception occurred", e);
        }
      } else if (klass instanceof Class<?>) {
        o = Utils.newInstance((Class<?>) klass);
      } else {
        throw new CruiseControlException("List contains element of type " + klass.getClass().getName()
                                             + ", expected String or Class");
      }
      if (!t.isInstance(o)) {
        throw new CruiseControlException(klass + " is not an instance of " + t.getName());
      }
      if (o instanceof CruiseControlConfigurable) {
        ((CruiseControlConfigurable) o).configure(configPairs);
      }
      objects.add(t.cast(o));
    }
    return objects;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AbstractConfig that = (AbstractConfig) o;

    return _originals.equals(that._originals);
  }

  @Override
  public int hashCode() {
    return _originals.hashCode();
  }

  /**
   * Marks keys retrieved via `get` as used. This is needed because `Configurable.configure` takes a `Map` instead
   * of an `AbstractConfig` and we can't change that without breaking public API like `Partitioner`.
   */
  private class RecordingMap<V> extends HashMap<String, V> {

    private final String _prefix;
    private final boolean _withIgnoreFallback;

    RecordingMap() {
      this("", false);
    }

    RecordingMap(String prefix, boolean withIgnoreFallback) {
      _prefix = prefix;
      _withIgnoreFallback = withIgnoreFallback;
    }

    RecordingMap(Map<String, ? extends V> m) {
      this(m, "", false);
    }

    RecordingMap(Map<String, ? extends V> m, String prefix, boolean withIgnoreFallback) {
      super(m);
      _prefix = prefix;
      _withIgnoreFallback = withIgnoreFallback;
    }

    @Override
    public V get(Object key) {
      if (key instanceof String) {
        String stringKey = (String) key;
        String keyWithPrefix;
        if (_prefix.isEmpty()) {
          keyWithPrefix = stringKey;
        } else {
          keyWithPrefix = _prefix + stringKey;
        }
        ignore(keyWithPrefix);
        if (_withIgnoreFallback) {
          ignore(stringKey);
        }
      }
      return super.get(key);
    }
  }
}
