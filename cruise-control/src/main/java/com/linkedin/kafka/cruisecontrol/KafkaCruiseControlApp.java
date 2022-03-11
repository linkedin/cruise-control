/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.codahale.metrics.jmx.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.async.AsyncKafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.metrics.LegacyObjectNameFactory;
import com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServlet;
import com.linkedin.kafka.cruisecontrol.servlet.security.CruiseControlSecurityHandler;
import com.linkedin.kafka.cruisecontrol.servlet.security.SecurityProvider;
import org.apache.kafka.common.config.types.Password;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.CustomRequestLog;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.Slf4jRequestLogWriter;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import javax.servlet.ServletException;
import java.util.List;

public class KafkaCruiseControlApp {

  private static final String METRIC_DOMAIN = "kafka.cruisecontrol";

  private final Server _server;
  private final KafkaCruiseControlConfig _config;
  private final AsyncKafkaCruiseControl _kafkaCruiseControl;
  private final JmxReporter _jmxReporter;

  KafkaCruiseControlApp(KafkaCruiseControlConfig config, Integer port, String hostname) throws ServletException {
    this._config = config;

    MetricRegistry metricRegistry = new MetricRegistry();
    _jmxReporter = JmxReporter.forRegistry(metricRegistry).inDomain(METRIC_DOMAIN)
            .createsObjectNamesWith(LegacyObjectNameFactory.getInstance()).build();
    _jmxReporter.start();

    _kafkaCruiseControl = new AsyncKafkaCruiseControl(config, metricRegistry);
    _server = new Server();
    if (_config.getBoolean(WebServerConfig.WEBSERVER_ACCESSLOG_ENABLED_CONFIG)) {
      // setup access logger
      Slf4jRequestLogWriter slf4jRequestLogWriter = new Slf4jRequestLogWriter();
      slf4jRequestLogWriter.setLoggerName(KafkaCruiseControlUtils.REQUESTLOG_LOGGER);
      CustomRequestLog requestLog = new CustomRequestLog(slf4jRequestLogWriter, CustomRequestLog.NCSA_FORMAT);
      _server.setRequestLog(requestLog);
    }
    _server.setConnectors(new Connector[]{ setupHttpConnector(hostname, port) });

    ServletContextHandler contextHandler = createContextHandler();
    maybeSetSecurityHandler(contextHandler);
    _server.setHandler(contextHandler);

    setupWebUi(contextHandler);

    KafkaCruiseControlServlet servlet = new KafkaCruiseControlServlet(_kafkaCruiseControl, metricRegistry);
    String apiUrlPrefix = config.getString(WebServerConfig.WEBSERVER_API_URLPREFIX_CONFIG);
    ServletHolder servletHolder = new ServletHolder(servlet);
    contextHandler.addServlet(servletHolder, apiUrlPrefix);
  }

  void start() throws Exception {
    _kafkaCruiseControl.startUp();
    _server.start();
    printStartupInfo();
  }

  void registerShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
  }

  void stop() {
    _kafkaCruiseControl.shutdown();
    _jmxReporter.close();
  }

  public String serverUrl() {
    return _server.getURI().toString();
  }

  private void printStartupInfo() {
    boolean corsEnabled = _config.getBoolean(WebServerConfig.WEBSERVER_HTTP_CORS_ENABLED_CONFIG);
    String webApiUrlPrefix = _config.getString(WebServerConfig.WEBSERVER_API_URLPREFIX_CONFIG);
    String uiUrlPrefix = _config.getString(WebServerConfig.WEBSERVER_UI_URLPREFIX_CONFIG);
    String webDir = _config.getString(WebServerConfig.WEBSERVER_UI_DISKPATH_CONFIG);
    String sessionPath = _config.getString(WebServerConfig.WEBSERVER_SESSION_PATH_CONFIG);
    System.out.println(">> ********************************************* <<");
    System.out.println(">> Application directory            : " + System.getProperty("user.dir"));
    System.out.println(">> REST API available on            : " + webApiUrlPrefix);
    System.out.println(">> Web UI available on              : " + uiUrlPrefix);
    System.out.println(">> Web UI Directory                 : " + webDir);
    System.out.println(">> Cookie prefix path               : " + sessionPath);
    System.out.println(">> Kafka Cruise Control started on  : " + serverUrl());
    System.out.println(">> CORS Enabled ?                   : " + corsEnabled);
    System.out.println(">> ********************************************* <<");
  }

  private ServerConnector setupHttpConnector(String hostname, int port) {
    ServerConnector serverConnector;
    Boolean webserverSslEnable = _config.getBoolean(WebServerConfig.WEBSERVER_SSL_ENABLE_CONFIG);
    if (webserverSslEnable != null && webserverSslEnable) {
      SslContextFactory sslServerContextFactory = new SslContextFactory.Server();
      sslServerContextFactory.setKeyStorePath(_config.getString(WebServerConfig.WEBSERVER_SSL_KEYSTORE_LOCATION_CONFIG));
      sslServerContextFactory.setKeyStorePassword(_config.getPassword(WebServerConfig.WEBSERVER_SSL_KEYSTORE_PASSWORD_CONFIG).value());
      Password sslKeyPassword = _config.getPassword(WebServerConfig.WEBSERVER_SSL_KEY_PASSWORD_CONFIG);
      if (sslKeyPassword != null) {
        sslServerContextFactory.setKeyManagerPassword(sslKeyPassword.value());
      }
      sslServerContextFactory.setProtocol(_config.getString(WebServerConfig.WEBSERVER_SSL_PROTOCOL_CONFIG));
      String keyStoreType = _config.getString(WebServerConfig.WEBSERVER_SSL_KEYSTORE_TYPE_CONFIG);
      if (keyStoreType != null) {
        sslServerContextFactory.setKeyStoreType(keyStoreType);
      }
      maybeConfigureTlsProtocolsAndCiphers(sslServerContextFactory);

      Boolean stsEnabled = _config.getBoolean(WebServerConfig.WEBSERVER_SSL_STS_ENABLED);
      if (stsEnabled != null && stsEnabled) {
        HttpConnectionFactory httpConnectionFactory = configureConnectionFactoryForHsts();
        serverConnector = new ServerConnector(_server, sslServerContextFactory, httpConnectionFactory);
      } else {
        serverConnector = new ServerConnector(_server, sslServerContextFactory);
      }
    } else {
      serverConnector = new ServerConnector(_server);
    }
    serverConnector.setHost(hostname);
    serverConnector.setPort(port);
    return serverConnector;
  }

  private HttpConnectionFactory configureConnectionFactoryForHsts() {
    Long stsMaxAge = _config.getLong(WebServerConfig.WEBSERVER_SSL_STS_MAX_AGE);
    Boolean stsIncludeSubDomains = _config.getBoolean(WebServerConfig.WEBSERVER_SSL_STS_INCLUDE_SUBDOMAINS);

    SecureRequestCustomizer src = new SecureRequestCustomizer();
    src.setStsMaxAge(stsMaxAge);
    src.setStsIncludeSubDomains(stsIncludeSubDomains);

    HttpConfiguration httpsConfig = new HttpConfiguration();
    httpsConfig.addCustomizer(src);

    return new HttpConnectionFactory(httpsConfig);
  }

  private void maybeConfigureTlsProtocolsAndCiphers(SslContextFactory sslContextFactory) {
    List<String> includeProtocols = _config.getList(WebServerConfig.WEBSERVER_SSL_INCLUDE_PROTOCOLS_CONFIG);
    if (includeProtocols != null) {
      sslContextFactory.setIncludeProtocols(includeProtocols.toArray(new String[0]));
    }
    List<String> excludeProtocols = _config.getList(WebServerConfig.WEBSERVER_SSL_EXCLUDE_PROTOCOLS_CONFIG);
    if (excludeProtocols != null) {
      sslContextFactory.setExcludeProtocols(excludeProtocols.toArray(new String[0]));
    }

    List<String> includeCiphers = _config.getList(WebServerConfig.WEBSERVER_SSL_INCLUDE_CIPHERS_CONFIG);
    if (includeCiphers != null) {
      sslContextFactory.setIncludeCipherSuites(includeCiphers.toArray(new String[0]));
    }

    List<String> excludeCiphers = _config.getList(WebServerConfig.WEBSERVER_SSL_EXCLUDE_CIPHERS_CONFIG);
    if (excludeCiphers != null) {
      sslContextFactory.setExcludeCipherSuites(excludeCiphers.toArray(new String[0]));
    }
  }

  private void setupWebUi(ServletContextHandler contextHandler) {
    // Placeholder for any static content
    String webuiDir = _config.getString(WebServerConfig.WEBSERVER_UI_DISKPATH_CONFIG);
    String webuiPathPrefix = _config.getString(WebServerConfig.WEBSERVER_UI_URLPREFIX_CONFIG);
    DefaultServlet defaultServlet = new DefaultServlet();
    ServletHolder holderWebapp = new ServletHolder("default", defaultServlet);
    // holderWebapp.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false");
    holderWebapp.setInitParameter("resourceBase", webuiDir);
    contextHandler.addServlet(holderWebapp, webuiPathPrefix);
  }

  private ServletContextHandler createContextHandler() {
    // Define context for servlet
    String sessionPath = _config.getString(WebServerConfig.WEBSERVER_SESSION_PATH_CONFIG);

    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath(sessionPath);
    return context;
  }

  private void maybeSetSecurityHandler(ServletContextHandler contextHandler) throws ServletException {
    SecurityProvider securityProvider = null;
    if (_config.getBoolean(WebServerConfig.WEBSERVER_SECURITY_ENABLE_CONFIG)) {
      securityProvider = _config.getConfiguredInstance(WebServerConfig.WEBSERVER_SECURITY_PROVIDER_CONFIG, SecurityProvider.class);
    }
    if (securityProvider != null) {
      securityProvider.init(_config);
      ConstraintSecurityHandler securityHandler = new CruiseControlSecurityHandler();
      securityHandler.setConstraintMappings(securityProvider.constraintMappings());
      securityHandler.setAuthenticator(securityProvider.authenticator());
      securityHandler.setLoginService(securityProvider.loginService());
      securityHandler.setRoles(securityProvider.roles());
      contextHandler.setSecurityHandler(securityHandler);
    }
  }
}
