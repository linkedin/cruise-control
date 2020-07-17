/*
 * Copyright 2019 LinkedIn Corp.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.custom;

import org.apache.http.auth.BasicUserPrincipal;
import org.eclipse.jetty.security.AbstractLoginService;
import org.eclipse.jetty.security.DefaultUserIdentity;
import org.eclipse.jetty.security.IdentityService;
import org.eclipse.jetty.server.UserIdentity;

import javax.security.auth.Subject;
import javax.servlet.ServletRequest;

public class CustomBasicLoginService extends AbstractLoginService {
    private static final String[] DEFAULT_ROLES = new String[]{"ADMIN"};
    private final String _authServiceUrl;
    private final String _authServiceToken;
    private final String _scope;
    private final String _grantType;

    public CustomBasicLoginService(String authServiceUrl, String authServiceToken, String scope, String grantType) {
        super();
        this._authServiceUrl = authServiceUrl;
        this._authServiceToken = authServiceToken;
        this._scope = scope;
        this._grantType = grantType;
    }

    @Override
    protected String[] loadRoleInfo(UserPrincipal user) {
        return new String[0];
    }

    @Override
    protected UserPrincipal loadUserInfo(String username) {
        return null;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public UserIdentity login(String username, Object credentials, ServletRequest request) {
        BasicUserPrincipal basicUserPrincipal = new BasicUserPrincipal(username);
        Subject subject = new Subject();
        subject.getPrincipals().add(basicUserPrincipal);

        if (CustomAuthClient.getInstance(_authServiceUrl,
                _authServiceToken,
                _scope,
                _grantType).verify(username, (String) credentials)) {
            return new DefaultUserIdentity(subject, basicUserPrincipal, DEFAULT_ROLES);
        }
        return null;
    }

    @Override
    public boolean validate(UserIdentity user) {
        return false;
    }

    @Override
    public IdentityService getIdentityService() {
        return _identityService;
    }

    @Override
    public void setIdentityService(IdentityService service) {

    }

    @Override
    public void logout(UserIdentity user) {

    }
}
