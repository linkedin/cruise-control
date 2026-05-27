/*
 * Copyright 2023 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.spnego;

import org.apache.kafka.common.security.kerberos.KerberosShortNamer;
import org.eclipse.jetty.security.ConfigurableSpnegoLoginService;
import org.eclipse.jetty.security.SpnegoUserIdentity;
import org.eclipse.jetty.security.SpnegoUserPrincipal;
import org.eclipse.jetty.security.authentication.AuthorizationService;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.server.UserIdentity.Scope;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.junit.Before;
import org.junit.Test;
import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.partialMockBuilder;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link SpnegoLoginServiceWithAuthServiceLifecycle}
 */
public class SpnegoLoginServiceWithAuthServiceLifecycleTest {
    public static final String USERNAME = "user1";
    private static final String REALM = "TEST_REALM";
    private static final String TOKEN = "TEST_TOKEN";
    private static final String ROLE = "ADMIN";
    private static final Subject SUBJECT = new Subject();
    private static final List<String> ATL_RULES = Collections.singletonList("RULE:[1:$1@$0](.*@.*)s/@.*/foo/");
    private final AuthorizationService _mockAuthorizationService = mock(AuthorizationService.class);
    private final ConfigurableSpnegoLoginService _mockLoginService = mock(ConfigurableSpnegoLoginService.class);
    private final HttpServletRequest _mockRequest = mock(HttpServletRequest.class);
    private final SpnegoUserIdentity _mockAuthIdentity = mock(SpnegoUserIdentity.class);
    private final UserIdentity _mockRoleIdentity = mock(UserIdentity.class);
    private final Scope _mockScope = mock(Scope.class);
    private final GSSContext _mockGSSContext = mock(GSSContext.class);

    /**
     * Init the unit test.
     */
    @Before
    public void setup() throws GSSException {
        expect(_mockLoginService.login(anyString(), anyObject(), anyObject())).andReturn(_mockAuthIdentity);
        expect(_mockAuthIdentity.getSubject()).andReturn(SUBJECT);
        expect(_mockRoleIdentity.isUserInRole(ROLE, _mockScope)).andReturn(true);
    }

    @Test
    public void testExtractSpnegoContext() throws ReflectiveOperationException {
        SpnegoLoginServiceWithAuthServiceLifecycle service = partialMockBuilder(SpnegoLoginServiceWithAuthServiceLifecycle.class).createMock();
        setField(service, "_spnegoLoginService", _mockLoginService);
        Class<?> contextClass = Class.forName("org.eclipse.jetty.security.ConfigurableSpnegoLoginService$SpnegoContext");
        Constructor<?> contextCtor = contextClass.getDeclaredConstructor();
        contextCtor.setAccessible(true);
        Object context = contextCtor.newInstance();
        Field contextField = ConfigurableSpnegoLoginService.class.getDeclaredField("_context");
        contextField.setAccessible(true);
        contextField.set(_mockLoginService, context);
        replay(service);

        service.extractSpnegoContext();
    }

    @Test
    public void testLoginWithoutKerberosRules() throws ReflectiveOperationException {
        SpnegoLoginServiceWithAuthServiceLifecycle service = createAuthServiceWithMocking(new SpnegoUserPrincipal(USERNAME, TOKEN));
        replay(service, _mockLoginService, _mockAuthorizationService, _mockAuthIdentity, _mockRoleIdentity);

        UserIdentity userIdentity = service.login(USERNAME, new Object(), _mockRequest);

        assertUserIdentity(USERNAME, userIdentity);
    }

    @Test
    public void testLoginWithKerberosRules() throws ReflectiveOperationException {
        String principalName = "user1@realm";
        String usernameReplaced = USERNAME + "foo";
        SpnegoUserPrincipal principal = new SpnegoUserPrincipal(principalName, TOKEN);
        SpnegoLoginServiceWithAuthServiceLifecycle service = createAuthServiceWithMocking(principalName, usernameReplaced, principal);
        setField(service, "_kerberosShortNamer", KerberosShortNamer.fromUnparsedRules(REALM, ATL_RULES));
        replay(service, _mockLoginService, _mockAuthorizationService, _mockAuthIdentity, _mockRoleIdentity);

        UserIdentity userIdentity = service.login(principalName, new Object(), _mockRequest);

        assertUserIdentity(usernameReplaced, userIdentity);
    }

    private SpnegoLoginServiceWithAuthServiceLifecycle createAuthServiceWithMocking(SpnegoUserPrincipal principal)
            throws ReflectiveOperationException {
        return createAuthServiceWithMocking(USERNAME, USERNAME, principal);
    }

    private SpnegoLoginServiceWithAuthServiceLifecycle createAuthServiceWithMocking(String name, String finalName, SpnegoUserPrincipal principal)
            throws ReflectiveOperationException {
        // Override getFullPrincipalFromGssContext and addContext as part of the partial
        // mock — possible because they were promoted from private to package-private in
        // the production class (see comments there). PowerMock's stubMethod(...) used to
        // reach into private methods via reflection; EasyMock's partial mock works via
        // Byte Buddy subclassing, which needs the override target to be at least
        // package-private.
        SpnegoLoginServiceWithAuthServiceLifecycle service =
            partialMockBuilder(SpnegoLoginServiceWithAuthServiceLifecycle.class)
                .addMockedMethod("getFullPrincipalFromGssContext", GSSContext.class)
                .addMockedMethod("addContext", HttpServletRequest.class)
                .createMock();
        expect(service.getFullPrincipalFromGssContext(_mockGSSContext)).andReturn(name);
        expect(service.addContext(_mockRequest)).andReturn(_mockGSSContext);

        setField(service, "_authorizationService", _mockAuthorizationService);
        setField(service, "_spnegoLoginService", _mockLoginService);

        expect(_mockAuthIdentity.getUserPrincipal()).andReturn(principal);
        expect(_mockAuthorizationService.getUserIdentity(_mockRequest, finalName)).andReturn(_mockRoleIdentity);

        return service;
    }

    private void assertUserIdentity(String username, UserIdentity userIdentity) {
        assertEquals(username, userIdentity.getUserPrincipal().getName());
        assertEquals(SUBJECT, userIdentity.getSubject());
        userIdentity.isUserInRole(ROLE, _mockScope);
        verify(_mockLoginService, _mockAuthorizationService, _mockRoleIdentity);
    }

    // Replacement for PowerMock's Whitebox.setInternalState. Plain reflection on
    // an in-package field works under JDK 17 without --add-opens because the
    // field lives in the unnamed module (our test classpath), not a JDK module.
    private static void setField(Object target, String name, Object value) throws ReflectiveOperationException {
        Class<?> cls = target.getClass();
        while (cls != null) {
            try {
                Field field = cls.getDeclaredField(name);
                field.setAccessible(true);
                field.set(target, value);
                return;
            } catch (NoSuchFieldException ignored) {
                cls = cls.getSuperclass();
            }
        }
        throw new NoSuchFieldException(name);
    }

}
