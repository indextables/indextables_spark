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

package com.tantivy4spark.utils

import com.tantivy4spark.TestBase
import com.tantivy4spark.testutils._
import org.apache.hadoop.conf.Configuration

import java.net.URI

/** Comprehensive unit tests for CredentialProviderFactory */
class CredentialProviderFactoryTest extends TestBase {

  private var hadoopConf: Configuration = _
  private var testUri: URI              = _

  override def beforeEach(): Unit = {
    hadoopConf = new Configuration()
    testUri = new URI("s3://test-bucket")
    // Clear cache before each test
    CredentialProviderFactory.clearCache()
  }

  override def afterEach(): Unit =
    // Clear cache after each test
    CredentialProviderFactory.clearCache()

  test("createV1Provider") {
    // Configure test credentials
    hadoopConf.set("test.aws.accessKey", "TEST_V1_ACCESS")
    hadoopConf.set("test.aws.secretKey", "TEST_V1_SECRET")
    hadoopConf.set("test.aws.sessionToken", "TEST_V1_SESSION")

    val provider = CredentialProviderFactory.createCredentialProvider(
      classOf[TestV1CredentialProvider].getName,
      testUri,
      hadoopConf
    )

    assert(provider != null)
    assert(provider.isInstanceOf[TestV1CredentialProvider])
    assert(CredentialProviderFactory.isV1Provider(provider))
    assert(!CredentialProviderFactory.isV2Provider(provider))
  }

  test("createV2Provider") {
    // Configure test credentials
    hadoopConf.set("test.aws.accessKey", "TEST_V2_ACCESS")
    hadoopConf.set("test.aws.secretKey", "TEST_V2_SECRET")
    hadoopConf.set("test.aws.sessionToken", "TEST_V2_SESSION")

    val provider = CredentialProviderFactory.createCredentialProvider(
      classOf[TestV2CredentialProvider].getName,
      testUri,
      hadoopConf
    )

    assert(provider != null)
    assert(provider.isInstanceOf[TestV2CredentialProvider])
    assert(!CredentialProviderFactory.isV1Provider(provider))
    assert(CredentialProviderFactory.isV2Provider(provider))
  }

  test("createDualProvider") {
    // Configure test credentials
    hadoopConf.set("test.aws.accessKey", "TEST_DUAL_ACCESS")
    hadoopConf.set("test.aws.secretKey", "TEST_DUAL_SECRET")

    val provider = CredentialProviderFactory.createCredentialProvider(
      classOf[TestDualCredentialProvider].getName,
      testUri,
      hadoopConf
    )

    assert(provider != null)
    assert(provider.isInstanceOf[TestDualCredentialProvider])
    // Dual provider implements both interfaces
    assert(CredentialProviderFactory.isV1Provider(provider))
    assert(CredentialProviderFactory.isV2Provider(provider))
  }

  test("extractV1Credentials") {
    hadoopConf.set("test.aws.accessKey", "V1_ACCESS_KEY")
    hadoopConf.set("test.aws.secretKey", "V1_SECRET_KEY")
    hadoopConf.set("test.aws.sessionToken", "V1_SESSION_TOKEN")

    val provider = CredentialProviderFactory.createCredentialProvider(
      classOf[TestV1CredentialProvider].getName,
      testUri,
      hadoopConf
    )

    val credentials = CredentialProviderFactory.extractCredentialsViaReflection(provider)

    assert(credentials != null)
    assert(credentials.accessKey == "V1_ACCESS_KEY")
    assert(credentials.secretKey == "V1_SECRET_KEY")
    assert(credentials.sessionToken.contains("V1_SESSION_TOKEN"))
    assert(credentials.hasSessionToken)
  }

  test("extractV2Credentials") {
    hadoopConf.set("test.aws.accessKey", "V2_ACCESS_KEY")
    hadoopConf.set("test.aws.secretKey", "V2_SECRET_KEY")
    hadoopConf.set("test.aws.sessionToken", "V2_SESSION_TOKEN")

    val provider = CredentialProviderFactory.createCredentialProvider(
      classOf[TestV2CredentialProvider].getName,
      testUri,
      hadoopConf
    )

    val credentials = CredentialProviderFactory.extractCredentialsViaReflection(provider)

    assert(credentials != null)
    assert(credentials.accessKey == "V2_ACCESS_KEY")
    assert(credentials.secretKey == "V2_SECRET_KEY")
    assert(credentials.sessionToken.contains("V2_SESSION_TOKEN"))
    assert(credentials.hasSessionToken)
  }

  test("extractCredentialsWithoutSessionToken") {
    hadoopConf.set("test.aws.accessKey", "BASIC_ACCESS_KEY")
    hadoopConf.set("test.aws.secretKey", "BASIC_SECRET_KEY")
    // No session token configured

    val provider = CredentialProviderFactory.createCredentialProvider(
      classOf[TestV1CredentialProvider].getName,
      testUri,
      hadoopConf
    )

    val credentials = CredentialProviderFactory.extractCredentialsViaReflection(provider)

    assert(credentials != null)
    assert(credentials.accessKey == "BASIC_ACCESS_KEY")
    assert(credentials.secretKey == "BASIC_SECRET_KEY")
    assert(credentials.sessionToken.isEmpty)
    assert(!credentials.hasSessionToken)
  }

  test("providerCaching") {
    hadoopConf.set("test.aws.accessKey", "CACHED_ACCESS_KEY")
    hadoopConf.set("test.aws.secretKey", "CACHED_SECRET_KEY")

    assert(CredentialProviderFactory.getCacheSize == 0)

    // Create first provider
    val provider1 = CredentialProviderFactory.createCredentialProvider(
      classOf[TestV1CredentialProvider].getName,
      testUri,
      hadoopConf
    )

    assert(CredentialProviderFactory.getCacheSize == 1)

    // Create second provider with same parameters - should get cached instance
    val provider2 = CredentialProviderFactory.createCredentialProvider(
      classOf[TestV1CredentialProvider].getName,
      testUri,
      hadoopConf
    )

    assert(CredentialProviderFactory.getCacheSize == 1)
    assert(provider1 eq provider2, "Should return same cached instance")
  }

  test("clearCache") {
    hadoopConf.set("test.aws.accessKey", "CACHE_TEST_ACCESS")
    hadoopConf.set("test.aws.secretKey", "CACHE_TEST_SECRET")

    // Create provider to populate cache
    CredentialProviderFactory.createCredentialProvider(
      classOf[TestV1CredentialProvider].getName,
      testUri,
      hadoopConf
    )

    assert(CredentialProviderFactory.getCacheSize == 1)

    // Clear cache
    CredentialProviderFactory.clearCache()

    assert(CredentialProviderFactory.getCacheSize == 0)
  }

  test("classNotFound") {
    val exception = intercept[IllegalArgumentException] {
      CredentialProviderFactory.createCredentialProvider(
        "com.nonexistent.Provider",
        testUri,
        hadoopConf
      )
    }

    assert(exception.getMessage.contains("not found"))
  }

  test("nullClassName") {
    val exception = intercept[IllegalArgumentException] {
      CredentialProviderFactory.createCredentialProvider(
        null,
        testUri,
        hadoopConf
      )
    }

    assert(exception.getMessage.contains("cannot be null or empty"))
  }

  test("emptyClassName") {
    val exception = intercept[IllegalArgumentException] {
      CredentialProviderFactory.createCredentialProvider(
        "",
        testUri,
        hadoopConf
      )
    }

    assert(exception.getMessage.contains("cannot be null or empty"))
  }

  test("whitespaceClassName") {
    val exception = intercept[IllegalArgumentException] {
      CredentialProviderFactory.createCredentialProvider(
        "   ",
        testUri,
        hadoopConf
      )
    }

    assert(exception.getMessage.contains("cannot be null or empty"))
  }

  test("invalidConstructorSignature") {
    val exception = intercept[IllegalArgumentException] {
      CredentialProviderFactory.createCredentialProvider(
        classOf[InvalidCredentialProvider].getName,
        testUri,
        hadoopConf
      )
    }

    assert(exception.getMessage.contains("must have constructor"))
  }

  test("unsupportedProviderInterface") {
    val exception = intercept[IllegalArgumentException] {
      CredentialProviderFactory.createCredentialProvider(
        classOf[UnsupportedProvider].getName,
        testUri,
        hadoopConf
      )
    }

    assert(exception.getMessage.contains("does not implement a supported"))
  }

  test("extractCredentialsFromNullProvider") {
    val exception = intercept[IllegalArgumentException] {
      CredentialProviderFactory.extractCredentialsViaReflection(null)
    }

    assert(exception.getMessage.contains("cannot be null"))
  }

  test("extractCredentialsFromUnsupportedProvider") {
    val unsupportedProvider = new UnsupportedProvider(testUri, hadoopConf)

    val exception = intercept[RuntimeException] {
      CredentialProviderFactory.extractCredentialsViaReflection(unsupportedProvider)
    }

    assert(exception.getMessage.contains("does not implement a supported"))
  }

  test("dualProviderExtractsAsV1") {
    hadoopConf.set("test.aws.accessKey", "DUAL_ACCESS_KEY")
    hadoopConf.set("test.aws.secretKey", "DUAL_SECRET_KEY")
    hadoopConf.set("test.aws.sessionToken", "DUAL_SESSION_TOKEN")

    val provider = CredentialProviderFactory.createCredentialProvider(
      classOf[TestDualCredentialProvider].getName,
      testUri,
      hadoopConf
    )

    // Should prioritize V1 extraction when both interfaces are present
    val credentials = CredentialProviderFactory.extractCredentialsViaReflection(provider)

    assert(credentials != null)
    assert(credentials.accessKey == "DUAL_ACCESS_KEY")
    assert(credentials.secretKey == "DUAL_SECRET_KEY")
    assert(credentials.sessionToken.contains("DUAL_SESSION_TOKEN"))
  }
}
