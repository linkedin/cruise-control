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

    private String _authServiceUrl;
    private String _authServiceToken;
    private String _scope;
    private String _grantType;

    @Override
    public void init(KafkaCruiseControlConfig config) {
        super.init(config);
        this._authServiceUrl = config.getString(WebServerConfig.CUSTOM_AUTHENTICATION_PROVIDER_URL_CONFIG);
        this._authServiceToken = config.getString(WebServerConfig.CUSTOM_AUTHENTICATION_KEY_CONFIG);
        this._scope = config.getString(WebServerConfig.CUSTOM_AUTHENTICATION_SCOPE_CONFIG);
        this._grantType = config.getString(WebServerConfig.CUSTOM_AUTHENTICATION_GRANT_TYPE_CONFIG);
    }

    @Override
    public LoginService loginService() {
        return new CustomBasicLoginService(_authServiceUrl, _authServiceToken, _scope, _grantType);
    }

    @Override
    public Authenticator authenticator() {
        return new CustomBasicAuthenticator();
    }
}
