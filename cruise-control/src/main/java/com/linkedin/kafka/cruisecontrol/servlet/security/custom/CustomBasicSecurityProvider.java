/*
 * Copyright 2019 LinkedIn Corp.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.custom;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.servlet.security.DefaultRoleSecurityProvider;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.LoginService;

public class CustomBasicSecurityProvider extends DefaultRoleSecurityProvider {

    private String authServiceUrl;
    private String authServiceToken;
    private String scope;
    private String grantType;

    @Override
    public void init(KafkaCruiseControlConfig config) {
        super.init(config);
        this.authServiceUrl = config.getString(WebServerConfig.CUSTOM_AUTHENTICATION_PROVIDER_URL_CONFIG);
        this.authServiceToken = config.getString(WebServerConfig.CUSTOM_AUTHENTICATION_KEY_CONFIG);
        this.scope = config.getString(WebServerConfig.CUSTOM_AUTHENTICATION_SCOPE_CONFIG);
        this.grantType = config.getString(WebServerConfig.CUSTOM_AUTHENTICATION_GRANT_TYPE_CONFIG);
    }

    @Override
    public LoginService loginService() {
        return new CustomBasicLoginService(authServiceUrl, authServiceToken, scope, grantType);
    }

    @Override
    public Authenticator authenticator() {
        return new CustomBasicAuthenticator();
    }
}
