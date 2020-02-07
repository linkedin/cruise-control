## Authentication

Cruise Control supports pluggable authentication methods via extending the
`com.linkedin.kafka.cruisecontrol.servlet.security.SecurityProvider` interface. The implementation should be configured
with the `webserver.security.provider` configuration. By default this is set to the
`com.linkedin.kafka.cruisecontrol.servlet.security.BasicSecurityProvider` class which provides a HTTP Basic
authentication. The configured authentication then can be enabled with the `webserver.security.enable=true` config in
the properties file.

### HTTP Basic

By configuring Cruise Control with the `BasicSecurityProvider`, the user will gain simple HTTP Basic
authentication where the users' credentials are stored in a file given by the `webserver.auth.credentials.file` config.
This file is assumed to be stored in a safe, protected location and only accessible by Cruise Control. The format of the
file follows Jetty's `HashLoginService`'s file format:
```username: password [,rolename ...]```

### Json Web Token authentication

Cruise Control can use token based authentication. To enable this, use the following configs:
* `webserver.security.enable`: This must be enabled to use the authentication features.
* `webserver.security.provider`: This must be changed to com.linkedin.kafka.cruisecontrol.servlet.security.jwt.JwtSecurityProvider.
* `webserver.auth.credentials.file`: It must point to a file in the `HashLoginService`'s file format introduced above
   but without the password part so `username: ,ROLE`. The possible roles are explained below.
* `jwt.authentication.provider.url`: This must point to the token prodiver's endpoint. It can contain a `{redirectUrl}`
   part which will be replaced by Cruise Control and points to the accessed Cruise Control endpoint.
* `jwt.cookie.name`: This can be set to provide a cookie name that points to a cookie issued by the token provider and
   which contains the JSON web token.
* `jwt.auth.certificate.location`: Tokens can be encrypted by the token provider and in this case there need to be a
  public key certificate that can be used for validating the token. This config points to a location that is assumed
  to be secured so that only Cruise Control can access it.

#### Configuring JWT authentication with Apache Knox
Testing with Apache Knox:
1. Download Knox (http://www.apache.org/dyn/closer.cgi/knox or https://knox.apache.org/books/knox-1-3-0/user-guide.html#Quick+Start)
2. `cd {KNOX_HOME}`
3. `bin/ldap.sh start`
4. Change `knoxsso.cookie.secure.only` to `false` in `conf/topologies/knoxsso.xml` to allow cookies over unsecured network
5. `bin/knoxcli.sh create-master`
6. `bin/gateway.sh start`
7. Create a credentials file containing this (for instance in `/tmp/test-roles.credentials`:
```
admin: ,ADMIN
sam: ,USER
```
8. `bin/knoxcli.sh create-cert` to create a validation certificate
9. `bin/knoxcli.sh export-cert` to export it (by default as PEM)
10. Start Cruise Control with the following extra configs:
```
webserver.security.enable=true
webserver.security.provider=com.linkedin.kafka.cruisecontrol.servlet.security.jwt.JwtSecurityProvider
jwt.authentication.provider.url=https://localhost:8443/gateway/idp/api/v1/websso?originalUrl={redirectUrl}
webserver.auth.credentials.file=/tmp/test-roles.credentials
jwt.cookie.name=hadoop-jwt
jwt.auth.certificate.location=/tmp/knox-1.3.0/data/security/keystores/gateway-identity.pem
```
11. Open http://localhost:9090/kafkacruisecontrol/state and see that it will redirect to the knox auth page (then log in
with the admin/admin-password username/password pair). It should redirect to the CC page. It is important to use the same
hostname (i.e. `localhost`) with CC otherwise Knox rejects the request.

## HTTPS

HTTPS can be configured with the following configs:
* `webserver.ssl.enable`: Enables or disables the HTTPS configuration.
* `webserver.ssl.keystore.location`: Sets the location of the keystore file.
* `webserver.ssl.keystore.password`: Sets the password for accessing the keystore file.
* `webserver.ssl.keystore.type`: Defines the type of the keystore (JKS, JCEKS, PKCS12, PKCS11 and DKS). It defaults
   to the Java version default.
* `webserver.ssl.key.password`: This is the password of the key stored in the keystore.
* `webserver.ssl.protocol`: The protocol to use for creating the SSL connection. By default it's "TLS".

## Authorization

By default Cruise Control defines three roles: VIEWER, USER and ADMIN.
* VIEWER role: has access to the most lightweight `kafka_cluster_state`, `user_tasks` and `review_board` endpoints.
* USER role: has access to all the GET endpoints except `bootstrap` and `train`.
* ADMIN role: has access to all endpoints.

It is possible to define custom roles too by extending the `SecurityProvider` interface. Furthermore if the default
role structure is required then it is usually easier to implement the `DefaultRoleSecurityProvider` class. With this
it is only required to define an authenticator and a login service.