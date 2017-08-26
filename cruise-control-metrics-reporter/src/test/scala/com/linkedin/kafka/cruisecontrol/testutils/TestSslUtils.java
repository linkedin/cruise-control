/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.kafka.cruisecontrol.testutils;

import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.network.Mode;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v1CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.crypto.params.AsymmetricKeyParameter;
import org.bouncycastle.crypto.util.PrivateKeyFactory;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.DefaultDigestAlgorithmIdentifierFinder;
import org.bouncycastle.operator.DefaultSignatureAlgorithmIdentifierFinder;
import org.bouncycastle.operator.bc.BcRSAContentSignerBuilder;

import javax.net.ssl.TrustManagerFactory;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is a copy of Apache Kafka o.a.k.test.TestSslUtils. We put it here to avoid depending on the test jar file of o.a.k.test.
 */
public class TestSslUtils {

  private TestSslUtils() {

  }

  /**
   * Create a self-signed X.509 Certificate.
   * From http://bfo.com/blog/2011/03/08/odds_and_ends_creating_a_new_x_509_certificate.html.
   *
   * @param dn        the X.509 Distinguished Name, eg "CN=Test, L=London, C=GB"
   * @param pair      the KeyPair
   * @param days      how many days from now the Certificate is valid for
   * @param algorithm the signing algorithm, eg "SHA1withRSA"
   * @return the self-signed certificate
   * @throws java.security.cert.CertificateException thrown if a security error or an IO error occurred.
   */
  public static X509Certificate generateCertificate(String dn, KeyPair pair,
                                                    int days, String algorithm)
      throws CertificateException {

    try {
      Security.addProvider(new BouncyCastleProvider());
      AlgorithmIdentifier sigAlgId = new DefaultSignatureAlgorithmIdentifierFinder().find(algorithm);
      AlgorithmIdentifier digAlgId = new DefaultDigestAlgorithmIdentifierFinder().find(sigAlgId);
      AsymmetricKeyParameter privateKeyAsymKeyParam = PrivateKeyFactory.createKey(pair.getPrivate().getEncoded());
      SubjectPublicKeyInfo subPubKeyInfo = SubjectPublicKeyInfo.getInstance(pair.getPublic().getEncoded());
      ContentSigner sigGen = new BcRSAContentSignerBuilder(sigAlgId, digAlgId).build(privateKeyAsymKeyParam);
      X500Name name = new X500Name(dn);
      Date from = new Date();
      Date to = new Date(from.getTime() + days * 86400000L);
      BigInteger sn = new BigInteger(64, new SecureRandom());

      X509v1CertificateBuilder v1CertGen = new X509v1CertificateBuilder(name, sn, from, to, name, subPubKeyInfo);
      X509CertificateHolder certificateHolder = v1CertGen.build(sigGen);
      return new JcaX509CertificateConverter().setProvider("BC").getCertificate(certificateHolder);
    } catch (CertificateException ce) {
      throw ce;
    } catch (Exception e) {
      throw new CertificateException(e);
    }
  }

  public static KeyPair generateKeyPair(String algorithm) throws NoSuchAlgorithmException {
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance(algorithm);
    keyGen.initialize(1024);
    return keyGen.genKeyPair();
  }

  private static KeyStore createEmptyKeyStore() throws GeneralSecurityException, IOException {
    KeyStore ks = KeyStore.getInstance("JKS");
    ks.load(null, null); // initialize
    return ks;
  }

  private static void saveKeyStore(KeyStore ks, String filename,
                                   Password password) throws GeneralSecurityException, IOException {
    try (FileOutputStream out = new FileOutputStream(filename)) {
      ks.store(out, password.value().toCharArray());
    }
  }

  public static void createKeyStore(String filename,
                                    Password password, String alias,
                                    Key privateKey, Certificate cert) throws GeneralSecurityException, IOException {
    KeyStore ks = createEmptyKeyStore();
    ks.setKeyEntry(alias, privateKey, password.value().toCharArray(),
        new Certificate[]{cert});
    saveKeyStore(ks, filename, password);
  }

  /**
   * Creates a keystore with a single key and saves it to a file.
   *
   * @param filename    String file to save
   * @param password    String store password to set on keystore
   * @param keyPassword String key password to set on key
   * @param alias       String alias to use for the key
   * @param privateKey  Key to save in keystore
   * @param cert        Certificate to use as certificate chain associated to key
   * @throws java.security.GeneralSecurityException for any error with the security APIs
   * @throws java.io.IOException                    if there is an I/O error saving the file
   */
  public static void createKeyStore(String filename,
                                    Password password, Password keyPassword, String alias,
                                    Key privateKey, Certificate cert) throws GeneralSecurityException, IOException {
    KeyStore ks = createEmptyKeyStore();
    ks.setKeyEntry(alias, privateKey, keyPassword.value().toCharArray(),
        new Certificate[]{cert});
    saveKeyStore(ks, filename, password);
  }

  public static <T extends Certificate> void createTrustStore(
      String filename, Password password, Map<String, T> certs) throws GeneralSecurityException, IOException {
    KeyStore ks = KeyStore.getInstance("JKS");
    try {
      FileInputStream in = new FileInputStream(filename);
      ks.load(in, password.value().toCharArray());
      in.close();
    } catch (EOFException e) {
      ks = createEmptyKeyStore();
    }
    for (Map.Entry<String, T> cert : certs.entrySet()) {
      ks.setCertificateEntry(cert.getKey(), cert.getValue());
    }
    saveKeyStore(ks, filename, password);
  }

  private static Map<String, Object> createSslConfig(Mode mode, File keyStoreFile, Password password, Password keyPassword,
                                                     File trustStoreFile, Password trustStorePassword) {
    Map<String, Object> sslConfigs = new HashMap<>();
    sslConfigs.put(SslConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.2"); // protocol to create SSLContext

    if (mode == Mode.SERVER || (mode == Mode.CLIENT && keyStoreFile != null)) {
      sslConfigs.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStoreFile.getPath());
      sslConfigs.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "JKS");
      sslConfigs.put(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG, TrustManagerFactory.getDefaultAlgorithm());
      sslConfigs.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, password);
      sslConfigs.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword);
    }

    sslConfigs.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStoreFile.getPath());
    sslConfigs.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, trustStorePassword);
    sslConfigs.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "JKS");
    sslConfigs.put(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG, TrustManagerFactory.getDefaultAlgorithm());

    List<String> enabledProtocols = new ArrayList<>();
    enabledProtocols.add("TLSv1.2");
    sslConfigs.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, enabledProtocols);

    return sslConfigs;
  }

  public static Map<String, Object> createSslConfig(boolean useClientCert, boolean trustStore, Mode mode, File trustStoreFile, String certAlias)
      throws IOException, GeneralSecurityException {
    return createSslConfig(useClientCert, trustStore, mode, trustStoreFile, certAlias, "localhost");
  }

  public static Map<String, Object> createSslConfig(boolean useClientCert, boolean trustStore, Mode mode, File trustStoreFile, String certAlias, String host)
      throws IOException, GeneralSecurityException {
    Map<String, X509Certificate> certs = new HashMap<>();
    File keyStoreFile = null;
    Password password = mode == Mode.SERVER ? new Password("ServerPassword") : new Password("ClientPassword");

    Password trustStorePassword = new Password("TrustStorePassword");

    if (mode == Mode.CLIENT && useClientCert) {
      keyStoreFile = File.createTempFile("clientKS", ".jks");
      KeyPair cKP = generateKeyPair("RSA");
      X509Certificate cCert = generateCertificate("CN=" + host + ", O=A client", cKP, 30, "SHA1withRSA");
      createKeyStore(keyStoreFile.getPath(), password, "client", cKP.getPrivate(), cCert);
      certs.put(certAlias, cCert);
      keyStoreFile.deleteOnExit();
    } else if (mode == Mode.SERVER) {
      keyStoreFile = File.createTempFile("serverKS", ".jks");
      KeyPair sKP = generateKeyPair("RSA");
      X509Certificate sCert = generateCertificate("CN=" + host + ", O=A server", sKP, 30,
          "SHA1withRSA");
      createKeyStore(keyStoreFile.getPath(), password, password, "server", sKP.getPrivate(), sCert);
      certs.put(certAlias, sCert);
      keyStoreFile.deleteOnExit();
    }

    if (trustStore) {
      createTrustStore(trustStoreFile.getPath(), trustStorePassword, certs);
      trustStoreFile.deleteOnExit();
    }

    return createSslConfig(mode, keyStoreFile, password, password, trustStoreFile, trustStorePassword);
  }

}
