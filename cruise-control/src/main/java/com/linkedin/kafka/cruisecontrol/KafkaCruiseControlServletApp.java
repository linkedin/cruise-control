/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.servlet.ServletRequestHandler;
import com.linkedin.kafka.cruisecontrol.servlet.security.CruiseControlSecurityHandler;
import com.linkedin.kafka.cruisecontrol.servlet.security.SecurityProvider;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.NCSARequestLog;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import javax.servlet.ServletException;
import java.util.List;

public class KafkaCruiseControlServletApp extends KafkaCruiseControlApp {

    private final Server _server;

    public KafkaCruiseControlServletApp(KafkaCruiseControlConfig config, Integer port, String hostname) throws ServletException {
        super(config, port, hostname);
        _server = new Server();
        NCSARequestLog requestLog = createRequestLog();
        if (requestLog != null) {
            _server.setRequestLog(requestLog);
        }
        _server.setConnectors(new Connector[]{ setupHttpConnector(hostname, port) });
        ServletContextHandler contextHandler = createContextHandler();
        maybeSetSecurityHandler(contextHandler);
        _server.setHandler(contextHandler);

        setupWebUi(contextHandler);

        ServletRequestHandler servlet = new ServletRequestHandler(_kafkaCruiseControl, _metricRegistry);
        String apiUrlPrefix = config.getString(WebServerConfig.WEBSERVER_API_URLPREFIX_CONFIG);
        ServletHolder servletHolder = new ServletHolder(servlet);
        contextHandler.addServlet(servletHolder, apiUrlPrefix);
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

    protected ServerConnector setupHttpConnector(String hostname, int port) {
        ServerConnector serverConnector;
        Boolean webserverSslEnable = _config.getBoolean(WebServerConfig.WEBSERVER_SSL_ENABLE_CONFIG);
        if (webserverSslEnable != null && webserverSslEnable) {
            SslContextFactory sslServerContextFactory = new SslContextFactory.Server();
            sslServerContextFactory.setKeyStorePath(_config.getString(WebServerConfig.WEBSERVER_SSL_KEYSTORE_LOCATION_CONFIG));
            sslServerContextFactory.setKeyStorePassword(_config.getPassword(WebServerConfig.WEBSERVER_SSL_KEYSTORE_PASSWORD_CONFIG).value());
            sslServerContextFactory.setKeyManagerPassword(_config.getPassword(WebServerConfig.WEBSERVER_SSL_KEY_PASSWORD_CONFIG).value());
            sslServerContextFactory.setProtocol(_config.getString(WebServerConfig.WEBSERVER_SSL_PROTOCOL_CONFIG));
            String keyStoreType = _config.getString(WebServerConfig.WEBSERVER_SSL_KEYSTORE_TYPE_CONFIG);
            if (keyStoreType != null) {
                sslServerContextFactory.setKeyStoreType(keyStoreType);
            }
            maybeConfigureTlsProtocolsAndCiphers(sslServerContextFactory);
            serverConnector = new ServerConnector(_server, sslServerContextFactory);
        } else {
            serverConnector = new ServerConnector(_server);
        }
        serverConnector.setHost(hostname);
        serverConnector.setPort(port);
        return serverConnector;
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

    protected void setupWebUi(ServletContextHandler contextHandler) {
        // Placeholder for any static content
        String webuiDir = _config.getString(WebServerConfig.WEBSERVER_UI_DISKPATH_CONFIG);
        String webuiPathPrefix = _config.getString(WebServerConfig.WEBSERVER_UI_URLPREFIX_CONFIG);
        DefaultServlet defaultServlet = new DefaultServlet();
        ServletHolder holderWebapp = new ServletHolder("default", defaultServlet);
        // holderWebapp.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false");
        holderWebapp.setInitParameter("resourceBase", webuiDir);
        contextHandler.addServlet(holderWebapp, webuiPathPrefix);
    }

    protected ServletContextHandler createContextHandler() {
        // Define context for servlet
        String sessionPath = _config.getString(WebServerConfig.WEBSERVER_SESSION_PATH_CONFIG);

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath(sessionPath);
        return context;
    }

    protected void maybeSetSecurityHandler(ServletContextHandler contextHandler) throws ServletException {
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

    protected NCSARequestLog createRequestLog() {
        boolean accessLogEnabled = _config.getBoolean(WebServerConfig.WEBSERVER_ACCESSLOG_ENABLED_CONFIG);
        if (accessLogEnabled) {
            String accessLogPath = _config.getString(WebServerConfig.WEBSERVER_ACCESSLOG_PATH_CONFIG);
            int accessLogRetention = _config.getInt(WebServerConfig.WEBSERVER_ACCESSLOG_RETENTION_DAYS_CONFIG);
            NCSARequestLog requestLog = new NCSARequestLog(accessLogPath);
            requestLog.setRetainDays(accessLogRetention);
            requestLog.setLogLatency(true);
            requestLog.setAppend(true);
            requestLog.setExtended(false);
            requestLog.setPreferProxiedForAddress(true);
            return requestLog;
        } else {
            return null;
        }
    }
    @Override
    public void start() throws Exception {
        _kafkaCruiseControl.startUp();
        _server.start();
        printStartupInfo();
    }

    @Override
    public String serverUrl() {
        return _server.getURI().toString();
    }
}
