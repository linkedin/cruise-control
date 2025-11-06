# Security Vulnerabilities

This document lists known security vulnerabilities and their fixes for dependencies used in this project.

## CVE Information

### Eclipse Jetty

**Issue Description:**  
In Eclipse Jetty versions 9.4.0 to 9.4.56 a buffer can be incorrectly released when confronted with a gzip error when inflating a request body. This can result in corrupted and/or inadvertent sharing of data between requests. This is a critical security vulnerability.

**Fix:**  
This CVE has been fixed in version 9.4.57.v20241219.

### Netty

**Issue Description:**  
Netty, an asynchronous, event-driven network application framework, has a vulnerability starting in version 4.1.91.Final and prior to version 4.1.118.Final.

**Fix:**  
Update to Netty version 4.1.118.Final or later to resolve this vulnerability.

## Reporting Security Issues

If you discover a security vulnerability in this project, please report it responsibly by following these steps:

1. Do not disclose the vulnerability publicly until it has been addressed.
2. Submit a detailed report including steps to reproduce the issue.
3. Allow time for the vulnerability to be addressed before public disclosure.

Thank you for helping to keep this project secure.