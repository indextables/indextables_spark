/*
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

package io.indextables.spark.auth

import io.indextables.spark.utils.CredentialProviderFactory
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.{
  AwsBasicCredentials,
  AwsCredentials,
  AwsCredentialsProvider,
  AwsSessionCredentials
}

/**
 * AWS SDK v2 AwsCredentialsProvider that wraps a v1 AWSCredentialsProvider.
 *
 * This adapter allows v1 credential providers (like UnityCatalogAWSCredentialProvider) to be used with the AWS SDK v2
 * S3Client while preserving the v1 provider's refresh and caching logic.
 *
 * The key insight is that the AWS SDK v2 calls `resolveCredentials()` each time credentials are needed, not just once
 * at client creation. This adapter delegates each call to the v1 provider's `getCredentials()` method, which allows the
 * v1 provider to:
 *   - Return cached credentials if still valid
 *   - Refresh credentials if they are near expiration
 *   - Fetch new credentials from the source (e.g., Unity Catalog API)
 *
 * Without this adapter, credentials would be extracted once at S3Client creation and wrapped in a
 * StaticCredentialsProvider, which cannot refresh. This causes "token expired" errors for long-running Spark jobs that
 * exceed the credential TTL (typically 1 hour for Unity Catalog temporary credentials).
 *
 * Usage:
 * {{{
 * val v1Provider = new UnityCatalogAWSCredentialProvider(uri, hadoopConf)
 * val v2Provider = new V1ToV2CredentialsProviderAdapter(v1Provider)
 * val s3Client = S3Client.builder()
 *   .credentialsProvider(v2Provider)
 *   .build()
 * }}}
 *
 * @param v1Provider
 *   The AWS SDK v1 AWSCredentialsProvider to wrap. Must implement com.amazonaws.auth.AWSCredentialsProvider interface.
 */
class V1ToV2CredentialsProviderAdapter(v1Provider: AnyRef) extends AwsCredentialsProvider {

  private val logger = LoggerFactory.getLogger(classOf[V1ToV2CredentialsProviderAdapter])

  // Validate that the provider is actually a v1 provider
  require(v1Provider != null, "v1Provider cannot be null")
  require(
    CredentialProviderFactory.isV1Provider(v1Provider),
    s"Provider must implement com.amazonaws.auth.AWSCredentialsProvider, got: ${v1Provider.getClass.getName}"
  )

  logger.info(s"Created V1ToV2CredentialsProviderAdapter wrapping: ${v1Provider.getClass.getName}")

  /**
   * Resolve credentials by delegating to the v1 provider's getCredentials() method.
   *
   * This method is called by the AWS SDK v2 each time credentials are needed for a request. By delegating to the v1
   * provider, we preserve its refresh logic:
   *   - The v1 provider checks if cached credentials are near expiration
   *   - If near expiration, it fetches fresh credentials from the source
   *   - Fresh credentials are cached and returned
   *
   * @return
   *   AWS credentials (either basic or session credentials)
   */
  override def resolveCredentials(): AwsCredentials = {
    logger.debug(s"resolveCredentials() called, delegating to v1 provider: ${v1Provider.getClass.getName}")

    // Extract credentials from the v1 provider using reflection
    // This calls the v1 provider's getCredentials() method, which may refresh
    // credentials if they are near expiration
    val creds = CredentialProviderFactory.extractCredentialsViaReflection(v1Provider)

    logger.debug(
      s"Resolved credentials: accessKey=${creds.accessKey.take(4)}..., " +
        s"hasSessionToken=${creds.hasSessionToken}"
    )

    // Convert to AWS SDK v2 credentials
    if (creds.hasSessionToken) {
      AwsSessionCredentials.create(creds.accessKey, creds.secretKey, creds.sessionToken.get)
    } else {
      AwsBasicCredentials.create(creds.accessKey, creds.secretKey)
    }
  }

  /** Get the wrapped v1 provider for debugging/testing purposes. */
  def getWrappedProvider: AnyRef = v1Provider
}
