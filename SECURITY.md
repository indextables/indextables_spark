# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in IndexTables for Spark, please
report it privately. **Do not file a public issue.**

### Preferred: GitHub Private Vulnerability Reporting

Use GitHub's private vulnerability reporting:
<https://github.com/indextables/indextables_spark/security/advisories/new>

This routes your report directly to the maintainers and keeps the details
confidential until a fix is available.

## What to Include

- A description of the issue and its potential impact
- Steps to reproduce, including affected versions, Spark version, and
  deployment environment (OSS Spark, Databricks, EMR, etc.)
- Any proof-of-concept code or test cases (where safe to share)
- Suggested mitigation, if known

## Response Process

The maintainers will acknowledge receipt of your report and work with you
on a coordinated disclosure timeline. Fixes are released as patch versions,
with a security advisory published once the fix is available.

## Supported Versions

Security fixes target the latest minor release line. Older versions are
supported on a best-effort basis.

## Scope

This policy covers the IndexTables Spark DataSource. Vulnerabilities in
the underlying tantivy4java library should be reported there:
<https://github.com/indextables/tantivy4java/security/advisories/new>
