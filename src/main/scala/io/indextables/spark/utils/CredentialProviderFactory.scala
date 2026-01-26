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

package io.indextables.spark.utils

import java.net.URI
import java.util.concurrent.ConcurrentHashMap

import scala.util.{Failure, Success, Try}

import org.apache.hadoop.conf.Configuration

import org.slf4j.LoggerFactory

/**
 * Factory for creating and managing AWS credential providers using reflection. This approach avoids compile-time
 * dependencies on specific AWS SDK versions, allowing the system to work with any AWS SDK v1 or v2 at runtime.
 */
object CredentialProviderFactory {

  private val logger = LoggerFactory.getLogger(CredentialProviderFactory.getClass)

  // Cache for instantiated providers to avoid repeated reflection overhead
  private val providerCache = new ConcurrentHashMap[String, AnyRef]()

  // Configuration key for custom credential provider class
  val CREDENTIAL_PROVIDER_CLASS_KEY = "spark.indextables.aws.credentialsProviderClass"

  /** Simple credential holder to avoid AWS SDK dependencies */
  case class BasicAWSCredentials(
    accessKey: String,
    secretKey: String,
    sessionToken: Option[String] = None) {
    def hasSessionToken: Boolean = sessionToken.exists(_.trim.nonEmpty)
  }

  /** Create a credential provider using reflection. Returns AnyRef to avoid AWS SDK dependencies. */
  def createCredentialProvider(
    providerClassName: String,
    uri: URI,
    hadoopConf: Configuration
  ): AnyRef = {
    require(
      providerClassName != null && providerClassName.trim.nonEmpty,
      "Credential provider class name cannot be null or empty"
    )

    val cacheKey = generateCacheKey(providerClassName, uri, hadoopConf)

    // Check cache first
    Option(providerCache.get(cacheKey)) match {
      case Some(cachedProvider) =>
        logger.debug(s"Using cached credential provider: $providerClassName")
        cachedProvider
      case None =>
        createNewProvider(providerClassName, uri, hadoopConf, cacheKey)
    }
  }

  private def createNewProvider(
    providerClassName: String,
    uri: URI,
    hadoopConf: Configuration,
    cacheKey: String
  ): AnyRef =
    Try {
      logger.info(s"Creating custom credential provider: $providerClassName")

      // Load the provider class
      val providerClass = Class.forName(providerClassName)
      logger.debug(s"Successfully loaded credential provider class: $providerClassName")

      // Find constructor with (URI, Configuration) signature
      val constructor = providerClass.getConstructor(classOf[URI], classOf[Configuration])

      // Instantiate the provider
      val provider = constructor.newInstance(uri, hadoopConf).asInstanceOf[AnyRef]
      logger.debug(s"Successfully instantiated credential provider: $providerClassName")

      // Validate that it implements a known credential provider interface
      if (!isValidCredentialProvider(provider)) {
        throw new IllegalArgumentException(
          s"Class $providerClassName does not implement a supported AWS credential provider interface. " +
            "Must implement com.amazonaws.auth.AWSCredentialsProvider (v1) or " +
            "software.amazon.awssdk.auth.credentials.AwsCredentialsProvider (v2)"
        )
      }

      // Cache the provider for future use
      providerCache.put(cacheKey, provider)
      logger.info(s"Successfully created and cached credential provider: $providerClassName")

      provider
    } match {
      case Success(provider) => provider
      case Failure(ex: ClassNotFoundException) =>
        logger.error(s"Credential provider class not found: $providerClassName", ex)
        throw new IllegalArgumentException(s"Credential provider class not found: $providerClassName", ex)
      case Failure(ex: NoSuchMethodException) =>
        logger.error(
          s"Credential provider class $providerClassName does not have required constructor (URI, Configuration)",
          ex
        )
        throw new IllegalArgumentException(
          s"Credential provider class $providerClassName must have constructor with signature: " +
            s"public $providerClassName(java.net.URI, org.apache.hadoop.conf.Configuration)",
          ex
        )
      case Failure(ex: IllegalArgumentException) =>
        // Re-throw IllegalArgumentException directly (e.g., from interface validation)
        throw ex
      case Failure(ex) =>
        logger.error(s"Failed to instantiate credential provider: $providerClassName", ex)
        throw new RuntimeException(s"Failed to create credential provider: ${ex.getMessage}", ex)
    }

  /** Extract credentials from a provider using reflection. Works with both v1 and v2 AWS SDK credential providers. */
  def extractCredentialsViaReflection(provider: AnyRef): BasicAWSCredentials = {
    require(provider != null, "Credential provider cannot be null")

    Try {
      // First try v1 SDK approach
      if (isV1Provider(provider)) {
        extractV1Credentials(provider)
      }
      // Then try v2 SDK approach
      else if (isV2Provider(provider)) {
        extractV2Credentials(provider)
      } else {
        throw new IllegalArgumentException(
          s"Provider does not implement a supported AWS credential provider interface: ${provider.getClass.getName}"
        )
      }
    } match {
      case Success(credentials) => credentials
      case Failure(ex) =>
        logger.error(s"Failed to extract credentials from provider: ${provider.getClass.getName}", ex)
        throw new RuntimeException(s"Failed to extract credentials: ${ex.getMessage}", ex)
    }
  }

  /** Check if provider implements AWS SDK v1 AWSCredentialsProvider interface */
  def isV1Provider(provider: AnyRef): Boolean =
    isAssignableFrom(provider.getClass, "com.amazonaws.auth.AWSCredentialsProvider")

  /** Check if provider implements AWS SDK v2 AwsCredentialsProvider interface */
  def isV2Provider(provider: AnyRef): Boolean =
    isAssignableFrom(provider.getClass, "software.amazon.awssdk.auth.credentials.AwsCredentialsProvider")

  /** Check if a class implements/extends a given interface/class by name (using reflection) */
  private def isAssignableFrom(clazz: Class[_], interfaceName: String): Boolean =
    Try {
      val interfaceClass = Class.forName(interfaceName)
      interfaceClass.isAssignableFrom(clazz)
    } match {
      case Success(result) => result
      case Failure(_) =>
        logger.debug(s"Interface/class not found on classpath: $interfaceName")
        false
    }

  /** Validate that the provider implements a supported credential provider interface */
  private def isValidCredentialProvider(provider: AnyRef): Boolean =
    isV1Provider(provider) || isV2Provider(provider)

  /** Extract credentials from AWS SDK v1 provider */
  private def extractV1Credentials(provider: AnyRef): BasicAWSCredentials = {
    logger.debug(s"Extracting credentials from v1 provider: ${provider.getClass.getName}")

    // Call getCredentials() method
    val getCredentialsMethod = provider.getClass.getMethod("getCredentials")
    val credentials          = getCredentialsMethod.invoke(provider).asInstanceOf[AnyRef]

    if (credentials == null) {
      throw new RuntimeException("Credential provider returned null credentials")
    }

    // Extract access key and secret key
    val getAWSAccessKeyId = credentials.getClass.getMethod("getAWSAccessKeyId")
    val getAWSSecretKey   = credentials.getClass.getMethod("getAWSSecretKey")

    val accessKey = getAWSAccessKeyId.invoke(credentials).asInstanceOf[String]
    val secretKey = getAWSSecretKey.invoke(credentials).asInstanceOf[String]

    // Try to extract session token (optional - may not exist for basic credentials)
    val sessionToken = Try {
      val getSessionToken = credentials.getClass.getMethod("getSessionToken")
      Option(getSessionToken.invoke(credentials).asInstanceOf[String])
    }.getOrElse {
      logger.debug("No session token method found - using basic credentials")
      None
    }

    if (accessKey == null || accessKey.trim.isEmpty) {
      throw new RuntimeException("Access key is null or empty")
    }
    if (secretKey == null || secretKey.trim.isEmpty) {
      throw new RuntimeException("Secret key is null or empty")
    }

    logger.debug(s"Successfully extracted v1 credentials. Access key: ${accessKey
        .take(4)}..., Session token: ${if (sessionToken.isDefined) "present" else "not present"}")

    BasicAWSCredentials(accessKey, secretKey, sessionToken)
  }

  /** Extract credentials from AWS SDK v2 provider */
  private def extractV2Credentials(provider: AnyRef): BasicAWSCredentials = {
    logger.debug(s"Extracting credentials from v2 provider: ${provider.getClass.getName}")

    // Call resolveCredentials() method
    val resolveCredentialsMethod = provider.getClass.getMethod("resolveCredentials")
    val credentials              = resolveCredentialsMethod.invoke(provider).asInstanceOf[AnyRef]

    if (credentials == null) {
      throw new RuntimeException("Credential provider returned null credentials")
    }

    // Extract access key and secret key
    val accessKeyId     = credentials.getClass.getMethod("accessKeyId")
    val secretAccessKey = credentials.getClass.getMethod("secretAccessKey")

    val accessKey = accessKeyId.invoke(credentials).asInstanceOf[String]
    val secretKey = secretAccessKey.invoke(credentials).asInstanceOf[String]

    // Try to extract session token (optional - may not exist for basic credentials)
    val sessionToken = Try {
      val sessionTokenMethod = credentials.getClass.getMethod("sessionToken")
      Option(sessionTokenMethod.invoke(credentials).asInstanceOf[String])
    }.getOrElse {
      logger.debug("No session token method found - using basic credentials")
      None
    }

    if (accessKey == null || accessKey.trim.isEmpty) {
      throw new RuntimeException("Access key is null or empty")
    }
    if (secretKey == null || secretKey.trim.isEmpty) {
      throw new RuntimeException("Secret key is null or empty")
    }

    logger.debug(s"Successfully extracted v2 credentials. Access key: ${accessKey
        .take(4)}..., Session token: ${if (sessionToken.isDefined) "present" else "not present"}")

    BasicAWSCredentials(accessKey, secretKey, sessionToken)
  }

  /** Generate cache key for provider instances */
  private def generateCacheKey(
    providerClassName: String,
    uri: URI,
    hadoopConf: Configuration
  ): String =
    // Simple cache key based on class name and URI
    // In production, you might want to include relevant hadoop config values
    s"$providerClassName:${uri.toString}"

  /** Clear the provider cache (useful for testing) */
  def clearCache(): Unit = {
    logger.debug("Clearing credential provider cache")
    providerCache.clear()
  }

  /** Get the size of the provider cache (useful for monitoring) */
  def getCacheSize: Int = providerCache.size()

  /**
   * Resolve AWS credentials with standard priority:
   *   1. Explicit credentials (if present - driver may have already resolved them) 2. Custom provider class (if
   *      configured) 3. Return None (caller should use default provider chain)
   *
   * @param accessKey
   *   Explicit access key (may be empty/null)
   * @param secretKey
   *   Explicit secret key (may be empty/null)
   * @param sessionToken
   *   Explicit session token (optional)
   * @param providerClassName
   *   Custom credential provider class (optional)
   * @param tablePath
   *   Table path for provider constructor
   * @param hadoopConf
   *   Hadoop configuration for provider
   * @return
   *   Some(credentials) if resolved, None if should use default chain
   */
  def resolveAWSCredentials(
    accessKey: Option[String],
    secretKey: Option[String],
    sessionToken: Option[String],
    providerClassName: Option[String],
    tablePath: String,
    hadoopConf: Configuration
  ): Option[BasicAWSCredentials] = {
    // Priority 1: Use explicit credentials if present
    (accessKey.filter(_.nonEmpty), secretKey.filter(_.nonEmpty)) match {
      case (Some(key), Some(secret)) =>
        logger.debug(s"Using explicit credentials: accessKey=${key.take(4)}...")
        return Some(BasicAWSCredentials(key, secret, sessionToken.filter(_.nonEmpty)))
      case _ => // continue to provider
    }

    // Priority 2: Use custom provider if configured
    providerClassName.filter(_.nonEmpty) match {
      case Some(className) =>
        try {
          val uri      = new URI(tablePath)
          val provider = createCredentialProvider(className, uri, hadoopConf)
          val creds    = extractCredentialsViaReflection(provider)
          logger.debug(s"Resolved credentials from provider $className: accessKey=${creds.accessKey.take(4)}...")
          Some(creds)
        } catch {
          case ex: Exception =>
            logger.warn(s"Failed to resolve credentials from provider $className: ${ex.getMessage}")
            None
        }
      case None =>
        // Priority 3: Return None, let caller use default provider chain
        None
    }
  }

  /**
   * Resolve AWS credentials from a config map with standard priority. Convenience method that extracts values from
   * config map.
   *
   * PERFORMANCE OPTIMIZATION: For UnityCatalogAWSCredentialProvider, this method uses the fast path
   * that creates the provider directly from the config Map, bypassing expensive Hadoop Configuration creation.
   */
  def resolveAWSCredentialsFromConfig(
    configs: Map[String, String],
    tablePath: String
  ): Option[BasicAWSCredentials] = {
    def getConfig(key: String): Option[String] =
      configs.get(key).orElse(configs.get(key.toLowerCase))

    // Priority 1: Use explicit credentials if present
    (getConfig("spark.indextables.aws.accessKey").filter(_.nonEmpty),
     getConfig("spark.indextables.aws.secretKey").filter(_.nonEmpty)) match {
      case (Some(key), Some(secret)) =>
        logger.debug(s"Using explicit credentials: accessKey=${key.take(4)}...")
        return Some(BasicAWSCredentials(key, secret, getConfig("spark.indextables.aws.sessionToken").filter(_.nonEmpty)))
      case _ => // continue to provider
    }

    // Priority 2: Use custom provider if configured
    getConfig("spark.indextables.aws.credentialsProviderClass").filter(_.nonEmpty) match {
      case Some(className) =>
        try {
          // Normalize path to table root for consistent cache keys
          val normalizedPath = io.indextables.spark.util.TablePathNormalizer.normalizeToTablePath(tablePath)
          val uri = new URI(normalizedPath)

          // FAST PATH: For UnityCatalogAWSCredentialProvider, use Map-based factory
          // This avoids creating Hadoop Configuration which is expensive
          val provider = if (className.contains("UnityCatalogAWSCredentialProvider")) {
            logger.debug(s"Using fast path for UnityCatalogAWSCredentialProvider with normalized path: $normalizedPath")
            io.indextables.spark.auth.unity.UnityCatalogAWSCredentialProvider.fromConfig(uri, configs)
          } else {
            // SLOW PATH: Other providers need Hadoop Configuration
            // Use cached Hadoop Configuration to reduce overhead
            val hadoopConf = io.indextables.spark.util.ConfigUtils.getOrCreateHadoopConfiguration(configs)
            createCredentialProvider(className, uri, hadoopConf)
          }

          val creds = extractCredentialsViaReflection(provider)
          logger.debug(s"Resolved credentials from provider $className: accessKey=${creds.accessKey.take(4)}...")
          Some(creds)
        } catch {
          case ex: Exception =>
            logger.warn(s"Failed to resolve credentials from provider $className: ${ex.getMessage}")
            None
        }
      case None =>
        // Priority 3: Return None, let caller use default provider chain
        None
    }
  }

  /**
   * Create a credential provider using the fast path (Map-based) when possible.
   *
   * For UnityCatalogAWSCredentialProvider, this uses the Map-based factory method
   * that bypasses Hadoop Configuration creation entirely.
   *
   * @param providerClassName The fully qualified class name of the provider
   * @param uri The URI for the table (will be normalized to table root)
   * @param configMap The configuration map
   * @return The credential provider instance
   */
  def createCredentialProviderFromConfig(
    providerClassName: String,
    uri: URI,
    configMap: Map[String, String]
  ): AnyRef = {
    require(
      providerClassName != null && providerClassName.trim.nonEmpty,
      "Credential provider class name cannot be null or empty"
    )

    // Normalize path to table root for consistent cache keys
    val normalizedUri = new URI(io.indextables.spark.util.TablePathNormalizer.normalizeToTablePath(uri.toString))

    // FAST PATH: For UnityCatalogAWSCredentialProvider, use Map-based factory
    if (providerClassName.contains("UnityCatalogAWSCredentialProvider")) {
      logger.info(s"Creating UnityCatalogAWSCredentialProvider via fast path (Map-based) for: $normalizedUri")
      return io.indextables.spark.auth.unity.UnityCatalogAWSCredentialProvider.fromConfig(normalizedUri, configMap)
    }

    // SLOW PATH: Other providers need Hadoop Configuration (use cached)
    logger.info(s"Creating credential provider via slow path (Hadoop Configuration): $providerClassName")
    val hadoopConf = io.indextables.spark.util.ConfigUtils.getOrCreateHadoopConfiguration(configMap)
    createCredentialProvider(providerClassName, normalizedUri, hadoopConf)
  }
}
