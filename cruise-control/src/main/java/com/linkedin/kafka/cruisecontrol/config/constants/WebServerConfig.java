/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config.constants;

import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;


/**
 * A class to keep Cruise Control Web Server Configs and defaults.
 * DO NOT CHANGE EXISTING CONFIG NAMES AS CHANGES WOULD BREAK USER CODE.
 */
public class WebServerConfig {
  private WebServerConfig() {

  }

  /**
   * <code>webserver.http.port</code>
   */
  public static final String WEBSERVER_HTTP_PORT_CONFIG = "webserver.http.port";
  public static final int DEFAULT_WEBSERVER_HTTP_PORT = 9090;
  public static final String WEBSERVER_HTTP_PORT_DOC = "Cruise Control Webserver bind port.";

  /**
   * <code>webserver.http.address</code>
   */
  public static final String WEBSERVER_HTTP_ADDRESS_CONFIG = "webserver.http.address";
  public static final String DEFAULT_WEBSERVER_HTTP_ADDRESS = "127.0.0.1";
  public static final String WEBSERVER_HTTP_ADDRESS_DOC = "Cruise Control Webserver bind ip address.";

  /**
   * <code>webserver.http.cors.enabled</code>
   */
  public static final String WEBSERVER_HTTP_CORS_ENABLED_CONFIG = "webserver.http.cors.enabled";
  public static final boolean DEFAULT_WEBSERVER_HTTP_CORS_ENABLED = false;
  public static final String WEBSERVER_HTTP_CORS_ENABLED_DOC = "CORS enablement flag. true if enabled, false otherwise";

  /**
   * <code>two.step.verification.enabled</code>
   */
  public static final String TWO_STEP_VERIFICATION_ENABLED_CONFIG = "two.step.verification.enabled";
  public static final boolean DEFAULT_TWO_STEP_VERIFICATION_ENABLED = false;
  public static final String TWO_STEP_VERIFICATION_ENABLED_DOC = "Enable two-step verification for processing POST requests.";

  /**
   * <code>webserver.http.cors.origin</code>
   */
  public static final String WEBSERVER_HTTP_CORS_ORIGIN_CONFIG = "webserver.http.cors.origin";
  public static final String DEFAULT_WEBSERVER_HTTP_CORS_ORIGIN = "*";
  public static final String WEBSERVER_HTTP_CORS_ORIGIN_DOC = "Value for the Access-Control-Allow-Origin header.";

  /**
   * <code>webserver.http.cors.allowmethods</code>
   */
  public static final String WEBSERVER_HTTP_CORS_ALLOWMETHODS_CONFIG = "webserver.http.cors.allowmethods";
  public static final String DEFAULT_WEBSERVER_HTTP_CORS_ALLOWMETHODS = "OPTIONS, GET, POST";
  public static final String WEBSERVER_HTTP_CORS_ALLOWMETHODS_DOC = "Value for the Access-Control-Request-Method header.";

  /**
   * <code>webserver.http.cors.exposeheaders</code>
   */
  public static final String WEBSERVER_HTTP_CORS_EXPOSEHEADERS_CONFIG = "webserver.http.cors.exposeheaders";
  public static final String DEFAULT_WEBSERVER_HTTP_CORS_EXPOSEHEADERS = "User-Task-ID";
  public static final String WEBSERVER_HTTP_CORS_EXPOSEHEADERS_DOC = "Value for the Access-Control-Expose-Headers header.";

  /**
   * <code>webserver.api.urlprefix</code>
   */
  public static final String WEBSERVER_API_URLPREFIX_CONFIG = "webserver.api.urlprefix";
  public static final String DEFAULT_WEBSERVER_API_URLPREFIX = "/kafkacruisecontrol/*";
  public static final String WEBSERVER_API_URLPREFIX_DOC = "REST API default url prefix";

  /**
   * <code>webserver.ui.diskpath</code>
   */
  public static final String WEBSERVER_UI_DISKPATH_CONFIG = "webserver.ui.diskpath";
  public static final String DEFAULT_WEBSERVER_UI_DISKPATH = "./cruise-control-ui/dist/";
  public static final String WEBSERVER_UI_DISKPATH_DOC = "Location where the Cruise Control frontend is deployed";

  /**
   * <code>webserver.ui.urlprefix</code>
   */
  public static final String WEBSERVER_UI_URLPREFIX_CONFIG = "webserver.ui.urlprefix";
  public static final String DEFAULT_WEBSERVER_UI_URLPREFIX = "/*";
  public static final String WEBSERVER_UI_URLPREFIX_DOC = "URL Path where UI is served from";

  /**
   * <code>webserver.request.maxBlockTimeMs</code>
   */
  public static final String WEBSERVER_REQUEST_MAX_BLOCK_TIME_MS_CONFIG = "webserver.request.maxBlockTimeMs";
  public static final long DEFAULT_WEBSERVER_REQUEST_MAX_BLOCK_TIME_MS = 10000L;
  public static final String WEBSERVER_REQUEST_MAX_BLOCK_TIME_MS_DOC = "Time after which request is converted to Async";

  /**
   * <code>webserver.session.maxExpiryTimeMs</code>
   */
  public static final String WEBSERVER_SESSION_EXPIRY_MS_CONFIG = "webserver.session.maxExpiryTimeMs";
  public static final long DEFAULT_WEBSERVER_SESSION_EXPIRY_MS = 60000L;
  public static final String WEBSERVER_SESSION_EXPIRY_MS_DOC = "Default Session Expiry Period";

  /**
   * <code>webserver.session.path</code>
   */
  public static final String WEBSERVER_SESSION_PATH_CONFIG = "webserver.session.path";
  public static final String DEFAULT_WEBSERVER_SESSION_PATH = "/";
  public static final String WEBSERVER_SESSION_PATH_DOC = "Default Session Path (for cookies)";

  /**
   * <code>webserver.accesslog.enabled</code>
   */
  public static final String WEBSERVER_ACCESSLOG_ENABLED_CONFIG = "webserver.accesslog.enabled";
  public static final boolean DEFAULT_WEBSERVER_ACCESSLOG_ENABLED = true;
  public static final String WEBSERVER_ACCESSLOG_ENABLED_DOC = "true if access log is enabled";


  /**
   * <code>webserver.accesslog.path</code>
   */
  public static final String WEBSERVER_ACCESSLOG_PATH_CONFIG = "webserver.accesslog.path";
  public static final String DEFAULT_WEBSERVER_ACCESSLOG_PATH = "access.log";
  public static final String WEBSERVER_ACCESSLOG_PATH_DOC = "HTTP Request log path";

  /**
   * <code>webserver.accesslog.retention.days</code>
   */
  public static final String WEBSERVER_ACCESSLOG_RETENTION_DAYS_CONFIG = "webserver.accesslog.retention.days";
  public static final int DEFAULT_WEBSERVER_ACCESSLOG_RETENTION_DAYS = 7;
  public static final String WEBSERVER_ACCESSLOG_RETENTION_DAYS_DOC = "HTTP Request log retention days";

  /**
   * <code>two.step.purgatory.retention.time.ms</code>
   */
  public static final String TWO_STEP_PURGATORY_RETENTION_TIME_MS_CONFIG = "two.step.purgatory.retention.time.ms";
  public static final long DEFAULT_TWO_STEP_PURGATORY_RETENTION_TIME_MS = TimeUnit.HOURS.toMillis(336);
  public static final String TWO_STEP_PURGATORY_RETENTION_TIME_MS_DOC = "The maximum time in milliseconds to "
      + "retain the requests in two-step (verification) purgatory.";

  /**
   * <code>two.step.purgatory.max.requests</code>
   */
  public static final String TWO_STEP_PURGATORY_MAX_REQUESTS_CONFIG = "two.step.purgatory.max.requests";
  public static final int DEFAULT_TWO_STEP_PURGATORY_MAX_REQUESTS = 25;
  public static final String TWO_STEP_PURGATORY_MAX_REQUESTS_DOC = "The maximum number of requests in two-step "
      + "(verification) purgatory.";

  /**
   * <code>max.active.user.tasks</code>
   */
  public static final String MAX_ACTIVE_USER_TASKS_CONFIG = "max.active.user.tasks";
  public static final int DEFAULT_MAX_ACTIVE_USER_TASKS = 5;
  public static final String MAX_ACTIVE_USER_TASKS_DOC = "The maximum number of user tasks for concurrently running in "
      + "async endpoints across all users.";

  /**
   * Define configs for Web Server.
   *
   * @param configDef Config definition.
   * @return The given ConfigDef after defining the configs for Web Server.
   */
  public static ConfigDef define(ConfigDef configDef) {
    return configDef.define(WEBSERVER_HTTP_PORT_CONFIG,
                            ConfigDef.Type.INT,
                            DEFAULT_WEBSERVER_HTTP_PORT,
                            atLeast(0),
                            ConfigDef.Importance.HIGH,
                            WEBSERVER_HTTP_PORT_DOC)
                    .define(WEBSERVER_HTTP_ADDRESS_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_WEBSERVER_HTTP_ADDRESS,
                            ConfigDef.Importance.HIGH,
                            WEBSERVER_HTTP_ADDRESS_DOC)
                    .define(WEBSERVER_HTTP_CORS_ENABLED_CONFIG,
                            ConfigDef.Type.BOOLEAN,
                            DEFAULT_WEBSERVER_HTTP_CORS_ENABLED,
                            ConfigDef.Importance.LOW,
                            WEBSERVER_HTTP_CORS_ENABLED_DOC)
                    .define(TWO_STEP_VERIFICATION_ENABLED_CONFIG,
                            ConfigDef.Type.BOOLEAN,
                            DEFAULT_TWO_STEP_VERIFICATION_ENABLED,
                            ConfigDef.Importance.MEDIUM,
                            TWO_STEP_VERIFICATION_ENABLED_DOC)
                    .define(WEBSERVER_HTTP_CORS_ORIGIN_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_WEBSERVER_HTTP_CORS_ORIGIN,
                            ConfigDef.Importance.LOW,
                            WEBSERVER_HTTP_CORS_ORIGIN_DOC)
                    .define(WEBSERVER_HTTP_CORS_ALLOWMETHODS_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_WEBSERVER_HTTP_CORS_ALLOWMETHODS,
                            ConfigDef.Importance.HIGH,
                            WEBSERVER_HTTP_CORS_ALLOWMETHODS_DOC)
                    .define(WEBSERVER_HTTP_CORS_EXPOSEHEADERS_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_WEBSERVER_HTTP_CORS_EXPOSEHEADERS,
                            ConfigDef.Importance.HIGH,
                            WEBSERVER_HTTP_CORS_EXPOSEHEADERS_DOC)
                    .define(WEBSERVER_API_URLPREFIX_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_WEBSERVER_API_URLPREFIX,
                            ConfigDef.Importance.HIGH,
                            WEBSERVER_API_URLPREFIX_DOC)
                    .define(WEBSERVER_UI_DISKPATH_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_WEBSERVER_UI_DISKPATH,
                            ConfigDef.Importance.MEDIUM,
                            WEBSERVER_UI_DISKPATH_DOC)
                    .define(WEBSERVER_UI_URLPREFIX_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_WEBSERVER_UI_URLPREFIX,
                            ConfigDef.Importance.MEDIUM,
                            WEBSERVER_UI_URLPREFIX_DOC)
                    .define(WEBSERVER_REQUEST_MAX_BLOCK_TIME_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            DEFAULT_WEBSERVER_REQUEST_MAX_BLOCK_TIME_MS,
                            atLeast(0L),
                            ConfigDef.Importance.HIGH,
                            WEBSERVER_REQUEST_MAX_BLOCK_TIME_MS_DOC)
                    .define(WEBSERVER_SESSION_EXPIRY_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            DEFAULT_WEBSERVER_SESSION_EXPIRY_MS,
                            atLeast(0L),
                            ConfigDef.Importance.HIGH,
                            WEBSERVER_SESSION_EXPIRY_MS_DOC)
                    .define(WEBSERVER_SESSION_PATH_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_WEBSERVER_SESSION_PATH,
                            ConfigDef.Importance.HIGH,
                            WEBSERVER_SESSION_PATH_DOC)
                    .define(WEBSERVER_ACCESSLOG_ENABLED_CONFIG,
                            ConfigDef.Type.BOOLEAN,
                            DEFAULT_WEBSERVER_ACCESSLOG_ENABLED,
                            ConfigDef.Importance.MEDIUM,
                            WEBSERVER_ACCESSLOG_ENABLED_DOC)
                    .define(WEBSERVER_ACCESSLOG_PATH_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_WEBSERVER_ACCESSLOG_PATH,
                            ConfigDef.Importance.LOW,
                            WEBSERVER_ACCESSLOG_PATH_DOC)
                    .define(WEBSERVER_ACCESSLOG_RETENTION_DAYS_CONFIG,
                            ConfigDef.Type.INT,
                            DEFAULT_WEBSERVER_ACCESSLOG_RETENTION_DAYS,
                            atLeast(0),
                            ConfigDef.Importance.LOW,
                            WEBSERVER_ACCESSLOG_RETENTION_DAYS_DOC)
                    .define(TWO_STEP_PURGATORY_RETENTION_TIME_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            DEFAULT_TWO_STEP_PURGATORY_RETENTION_TIME_MS,
                            atLeast(TimeUnit.HOURS.toMillis(1)),
                            ConfigDef.Importance.MEDIUM,
                            TWO_STEP_PURGATORY_RETENTION_TIME_MS_DOC)
                    .define(TWO_STEP_PURGATORY_MAX_REQUESTS_CONFIG,
                            ConfigDef.Type.INT,
                            DEFAULT_TWO_STEP_PURGATORY_MAX_REQUESTS,
                            atLeast(1),
                            ConfigDef.Importance.MEDIUM,
                            TWO_STEP_PURGATORY_MAX_REQUESTS_DOC)
                    .define(MAX_ACTIVE_USER_TASKS_CONFIG,
                            ConfigDef.Type.INT,
                            DEFAULT_MAX_ACTIVE_USER_TASKS,
                            atLeast(1),
                            ConfigDef.Importance.HIGH,
                            MAX_ACTIVE_USER_TASKS_DOC);
  }
}
