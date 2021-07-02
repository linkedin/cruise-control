/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.jwt;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;
import java.util.Date;
import java.util.List;

final class TokenGenerator {

  static final class TokenAndKeys {
    private final String _token;
    private final RSAPrivateKey _privateKey;
    private final RSAPublicKey _publicKey;

    private TokenAndKeys(String token, RSAPrivateKey privateKey, RSAPublicKey publicKey) {
      _token = token;
      _privateKey = privateKey;
      _publicKey = publicKey;
    }

    String token() {
      return _token;
    }

    RSAPublicKey publicKey() {
      return _publicKey;
    }

    RSAPrivateKey privateKey() {
      return _privateKey;
    }
  }

  private TokenGenerator() {
  }

  static TokenAndKeys generateToken(String subject) throws JOSEException {
    return generateToken(subject, null, -1);
  }

  static TokenAndKeys generateToken(String subject, long expirationTime) throws JOSEException {
    return generateToken(subject, null, expirationTime);
  }

  static TokenAndKeys generateToken(String subject, List<String> audience) throws JOSEException {
    return generateToken(subject, audience, -1);
  }

  static TokenAndKeys generateToken(String subject, List<String> audience, long expirationTime) throws JOSEException {
    RSAKey rsaJwk = new RSAKeyGenerator(2048)
        .keyID("123")
        .generate();
    RSAKey rsaPublicJWK = rsaJwk.toPublicJWK();
    RSASSASigner signer = new RSASSASigner(rsaJwk);

    JWSHeader header = new JWSHeader.Builder(JWSAlgorithm.RS256)
        .type(JOSEObjectType.JWT)
        .build();
    JWTClaimsSet.Builder claimsSet = new JWTClaimsSet.Builder()
        .subject(subject)
        .issuer("https://linkedin.com");

    if (audience != null) {
      claimsSet.audience(audience);
    }

    if (expirationTime > 0) {
      claimsSet.expirationTime(new Date(expirationTime));
    } else {
      claimsSet.expirationTime(Date.from(Instant.now().plusSeconds(120)));
    }

    SignedJWT signedJWT = new SignedJWT(header, claimsSet.build());
    signedJWT.sign(signer);

    return new TokenAndKeys(signedJWT.serialize(), (RSAPrivateKey) signer.getPrivateKey(), rsaPublicJWK.toRSAPublicKey());
  }
}
