/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.jwt;

import com.nimbusds.jwt.SignedJWT;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.security.ServerAuthException;
import org.eclipse.jetty.security.UserAuthentication;
import org.eclipse.jetty.security.authentication.LoginAuthenticator;
import org.eclipse.jetty.server.Authentication;
import org.eclipse.jetty.server.UserIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.text.ParseException;
import java.util.function.Function;

/**
 * <p>The {@link JwtAuthenticator} adds SSO capabilities to Cruise Control. The expected token is a Json Web Token (JWT).
 * This class should be used with {@link JwtLoginService} as the token check is carried out by that one. This class
 * handles redirects for unauthenticated requests and CORS preflight requests.</p>
 * The workflow can be described with the following diagram:
 * <pre>
 *       Client -----1., 4.----&gt; Cruise Control
 *        |  ^                        |
 *        |  |________2.______________|
 *        |
 *        |
 *        |------3.------------&gt; Authentication
 *                                 provider
 * </pre>
 * <ol>
 * <li>The client makes an initial call to Cruise Control
 * <li>If the request doesn't have a JWT cookie by the specified cookie name, it will be redirected to the authentication
 *     service to obtain it. If the request is an OPTIONS request we presume it's a CORS preflight request so it'll skip
 *     the authentication (if the user is authenticated at this point we'll use the existing credentials).
 * <li>The client authenticates with the provider and obtains the SSO token.
 * <li>The client can present the JWT cookie to Cruise Control. Cruise Control will validate the cookie with the
 *    {@link JwtLoginService} by checking its signature, audience and expiration.
 * </ol>
 */
public class JwtAuthenticator extends LoginAuthenticator {

  public static final String JWT_TOKEN_REQUEST_ATTRIBUTE = "com.linkedin.kafka.cruisecontrol.JwtTokenAttribute";
  static final Logger JWT_LOGGER = LoggerFactory.getLogger("kafka.cruisecontrol.jwt.logger");

  private static final String METHOD = "JWT";
  static final String BEARER = "Bearer";
  static final String REDIRECT_URL = "{redirectUrl}";

  private final String _cookieName;
  private final Function<HttpServletRequest, String> _authenticationProviderUrlGenerator;

  /**
   * Creates a new {@link JwtAuthenticator} instance with a custom authentication provider url and a cookie name that
   * will be populated by the authentication service with the JWT token.
   * @param authenticationProviderUrl is the HTTP(S) address of the authentication service. It will be used to create
   *                                  the redirection url. For instance <code>https://www.my-auth-service.com/websso?origin={redirectUrl}</code>
   *                                  will generate <code>https://www.my-auth-service.com/websso?origin=https://www.cruise-control.cc/state</code>
   *                                  which should redirect from <code>my-auth-service.com</code> to <code>cruise-control.cc/state</code>
   *                                  after obtaining the JWT token.
   * @param cookieName is the cookie name which will contain the cookie obtained from the authentication service.
   *                   <code>null</code> is an acceptable value when the token is always returned
   */
  public JwtAuthenticator(String authenticationProviderUrl, String cookieName) {
    _cookieName = cookieName;
    Function<String, Function<HttpServletRequest, String>> urlGen =
        url -> req -> url.replace(REDIRECT_URL, req.getRequestURL().toString() + getOriginalQueryString(req));
    _authenticationProviderUrlGenerator = urlGen.apply(authenticationProviderUrl);
  }

  @Override
  public String getAuthMethod() {
    return METHOD;
  }

  @Override
  public void prepareRequest(ServletRequest request) {

  }

  @Override
  public Authentication validateRequest(ServletRequest request, ServletResponse response, boolean mandatory) throws ServerAuthException {
    JWT_LOGGER.trace("Authentication request received for " + request.toString());
    if (!(request instanceof HttpServletRequest) && !(response instanceof HttpServletResponse)) {
      return Authentication.UNAUTHENTICATED;
    }

    String serializedJWT;
    HttpServletRequest req = (HttpServletRequest) request;
    // we'll skip the authentication for CORS preflight requests
    if (HttpMethod.OPTIONS.name().equalsIgnoreCase(req.getMethod())) {
      return Authentication.NOT_CHECKED;
    }
    serializedJWT = getJwtFromBearerAuthorization(req);
    if (serializedJWT == null) {
      serializedJWT = getJwtFromCookie(req);
    }
    if (serializedJWT == null) {
      String loginURL = _authenticationProviderUrlGenerator.apply(req);
      JWT_LOGGER.info("No JWT token found, sending redirect to " + loginURL);
      try {
        ((HttpServletResponse) response).sendRedirect(loginURL);
        return Authentication.SEND_CONTINUE;
      } catch (IOException e) {
        JWT_LOGGER.error("Couldn't authenticate request", e);
        throw new ServerAuthException(e);
      }
    } else {
      try {
        SignedJWT jwtToken = SignedJWT.parse(serializedJWT);
        String userName = jwtToken.getJWTClaimsSet().getSubject();
        request.setAttribute(JWT_TOKEN_REQUEST_ATTRIBUTE, serializedJWT);
        UserIdentity identity = login(userName, jwtToken, request);
        if (identity == null) {
          ((HttpServletResponse) response).setStatus(HttpStatus.UNAUTHORIZED_401);
          return Authentication.SEND_FAILURE;
        } else {
          return new UserAuthentication(getAuthMethod(), identity);
        }
      } catch (ParseException pe) {
        String loginURL = _authenticationProviderUrlGenerator.apply(req);
        JWT_LOGGER.warn("Unable to parse the JWT token, redirecting back to the login page", pe);
        try {
          ((HttpServletResponse) response).sendRedirect(loginURL);
        } catch (IOException e) {
          throw new ServerAuthException(e);
        }
      }
    }

    return Authentication.SEND_FAILURE;
  }

  @Override
  public boolean secureResponse(ServletRequest request, ServletResponse response, boolean mandatory, Authentication.User validatedUser) {
    return true;
  }

  String getJwtFromCookie(HttpServletRequest req) {
    String serializedJWT = null;
    Cookie[] cookies = req.getCookies();
    if (cookies != null) {
      for (Cookie cookie : cookies) {
        if (_cookieName != null && _cookieName.equals(cookie.getName())) {
          JWT_LOGGER.trace(_cookieName + " cookie has been found and is being processed");
          serializedJWT = cookie.getValue();
          break;
        }
      }
    }
    return serializedJWT;
  }

  String getJwtFromBearerAuthorization(HttpServletRequest req) {
    String authorizationHeader = req.getHeader(HttpHeader.AUTHORIZATION.asString());
    if (authorizationHeader == null || !authorizationHeader.startsWith(BEARER)) {
      return null;
    } else {
      return authorizationHeader.substring(BEARER.length()).trim();
    }
  }

  private String getOriginalQueryString(HttpServletRequest request) {
    String originalQueryString = request.getQueryString();
    return (originalQueryString == null) ? "" : "?" + originalQueryString;
  }
}
