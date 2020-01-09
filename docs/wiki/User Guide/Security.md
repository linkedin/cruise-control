## Authentication

Cruise Control supports pluggable authentication methods via extending the
`com.linkedin.kafka.cruisecontrol.servlet.security.SecurityProvider` interface. The implementation should be configured
with the `webserver.security.provider` configuration. By default this is set to the
`com.linkedin.kafka.cruisecontrol.servlet.security.BasicSecurityProvider` class which provides a HTTP Basic
authentication. The configured authentication then can be enabled with the `webserver.security.enable=true` config in
the properties file.

### HTTP Basic

By configuring Cruise Control with the `BasicSecurityProvider`, the user will gain simple HTTP Basic
authentication where the users' credentials are stored in a file given by the `basic.auth.credentials.file` config.
This file is assumed to be stored in a safe, protected location and only accessible by Cruise Control. The format of the
file follows Jetty's `HashLoginService`'s file format:
```username: password [,rolename ...]```

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