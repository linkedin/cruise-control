/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.jwt;

import com.linkedin.kafka.cruisecontrol.CruiseControlIntegrationTestHarness;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.Provider;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.STATE;
import static com.linkedin.kafka.cruisecontrol.servlet.security.SecurityTestUtils.AUTH_CREDENTIALS_FILE;
import static com.linkedin.kafka.cruisecontrol.servlet.security.SecurityTestUtils.BASIC_AUTH_CREDENTIALS_FILE;
import static org.junit.Assert.assertEquals;

public class JwtSecurityProviderIntegrationTest extends CruiseControlIntegrationTestHarness {

  private static final String CRUISE_CONTROL_STATE_ENDPOINT = "kafkacruisecontrol/" + STATE;
  private static final String TEST_USERNAME_KEY = "username";
  private static final String TEST_PASSWORD_KEY = "password";
  private static final String TEST_USERNAME = "ccTestUser";
  private static final String TEST_PASSWORD = "TestPwd123";
  private static final String ORIGIN = "origin";
  public static final String JWT_TOKEN_COOKIE_NAME = "jwt_token";

  private final TokenGenerator.TokenAndKeys _tokenAndKeys;
  private final Server _tokenProviderServer;
  private final File _publicKeyFile;

  class TestAuthenticatorHandler extends AbstractHandler {

    private final HashLoginService _loginService;

    TestAuthenticatorHandler() {
      _loginService = new HashLoginService();
      _loginService.setConfig(
          Objects.requireNonNull(this.getClass().getClassLoader().getResource(BASIC_AUTH_CREDENTIALS_FILE)).getPath());
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException {
      String username = request.getParameter(TEST_USERNAME_KEY);
      String password = request.getParameter(TEST_PASSWORD_KEY);

      String cruiseControlUrl = request.getParameter(ORIGIN);

      System.out.println(String.format("Handling login: %s %s %s", username, password, cruiseControlUrl));
      UserIdentity identity = _loginService.login(username, password, request);
      if (identity != null) {
        response.addCookie(new Cookie(JWT_TOKEN_COOKIE_NAME, _tokenAndKeys.token()));
      } else {
        response.sendError(HttpServletResponse.SC_FORBIDDEN);
      }
    }

    @Override
    protected void doStart() throws Exception {
      super.doStart();
      _loginService.start();
    }

    @Override
    protected void doStop() throws Exception {
      _loginService.stop();
      super.doStop();
    }
  }

  public JwtSecurityProviderIntegrationTest() throws Exception {
    _tokenAndKeys = TokenGenerator.generateToken(TEST_USERNAME);
    _publicKeyFile = createCertificate(_tokenAndKeys);
    _tokenProviderServer = new Server(0);
    _tokenProviderServer.setHandler(new TestAuthenticatorHandler());
  }

  /**
   * Starts the token provider and the Kafka environment before test
   * @throws Exception
   */
  @Before
  public void setup() throws Exception {
    _tokenProviderServer.start();
    super.start();
  }

  /**
   * Stops the token provider and the Kafka environment after test
   * @throws Exception
   */
  @After
  public void teardown() throws Exception {
    super.stop();
    _tokenProviderServer.stop();
  }

  @Override
  protected Map<String, Object> withConfigs() {
    Map<String, Object> securityConfigs = new HashMap<>();
    securityConfigs.put(WebServerConfig.WEBSERVER_SECURITY_ENABLE_CONFIG, true);
    securityConfigs.put(WebServerConfig.WEBSERVER_SECURITY_PROVIDER_CONFIG, JwtSecurityProvider.class);
    securityConfigs.put(WebServerConfig.JWT_AUTHENTICATION_PROVIDER_URL_CONFIG,
                        _tokenProviderServer.getURI().toString() + "?" + TEST_USERNAME_KEY + "=" + TEST_USERNAME + "&" + TEST_PASSWORD_KEY
                        + "=" + TEST_PASSWORD + "&origin=" + JwtAuthenticator.REDIRECT_URL);
    securityConfigs.put(WebServerConfig.WEBSERVER_AUTH_CREDENTIALS_FILE_CONFIG,
        Objects.requireNonNull(this.getClass().getClassLoader().getResource(AUTH_CREDENTIALS_FILE)).getPath());
    securityConfigs.put(WebServerConfig.JWT_COOKIE_NAME_CONFIG, JWT_TOKEN_COOKIE_NAME);
    securityConfigs.put(WebServerConfig.JWT_AUTH_CERTIFICATE_LOCATION_CONFIG, _publicKeyFile.getAbsolutePath());

    return securityConfigs;
  }

  @Test
  public void testSuccessfulLogin() throws Exception {
    HttpURLConnection stateEndpointConnection = (HttpURLConnection) new URI(_app.serverUrl())
        .resolve(CRUISE_CONTROL_STATE_ENDPOINT).toURL().openConnection();
    String cookie = stateEndpointConnection.getHeaderField(HttpHeader.SET_COOKIE.asString());
    stateEndpointConnection = (HttpURLConnection) new URI(_app.serverUrl())
        .resolve(CRUISE_CONTROL_STATE_ENDPOINT).toURL().openConnection();
    stateEndpointConnection.setRequestProperty(HttpHeader.COOKIE.asString(), cookie);
    assertEquals(HttpServletResponse.SC_OK, stateEndpointConnection.getResponseCode());
  }

  private File createCertificate(TokenGenerator.TokenAndKeys tokenAndKeys) throws Exception {
    String subjectDN = "C=US, ST=California, L=Santa Clara, O=LinkedIn, CN=localhost";
    Provider bcProvider = new BouncyCastleProvider();
    Security.addProvider(bcProvider);

    long now = System.currentTimeMillis();
    Date startDate = new Date(now);

    X500Name dnName = new X500Name(subjectDN);
    BigInteger certSerialNumber = new BigInteger(Long.toString(now));

    Calendar calendar = Calendar.getInstance();
    calendar.setTime(startDate);
    calendar.add(Calendar.YEAR, 100);

    Date endDate = calendar.getTime();
    String signatureAlgorithm = "SHA256WithRSA";
    ContentSigner contentSigner = new JcaContentSignerBuilder(signatureAlgorithm).build(tokenAndKeys.privateKey());

    JcaX509v3CertificateBuilder certBuilder = new JcaX509v3CertificateBuilder(
        dnName, certSerialNumber, startDate, endDate, dnName, tokenAndKeys.publicKey());

    X509Certificate cert = new JcaX509CertificateConverter().setProvider(bcProvider).getCertificate(certBuilder.build(contentSigner));

    File certificate = File.createTempFile("test-certificate", ".pub");

    try (OutputStream os = new FileOutputStream(certificate)) {
      Base64.Encoder encoder = Base64.getEncoder();
      os.write("-----BEGIN CERTIFICATE-----\n".getBytes(StandardCharsets.UTF_8));
      os.write(encoder.encodeToString(cert.getEncoded()).getBytes(StandardCharsets.UTF_8));
      os.write("\n-----END CERTIFICATE-----\n".getBytes(StandardCharsets.UTF_8));
    }

    return certificate;
  }
}
