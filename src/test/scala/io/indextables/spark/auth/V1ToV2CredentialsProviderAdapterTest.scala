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

import java.net.URI
import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.conf.Configuration

import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, BasicAWSCredentials, BasicSessionCredentials}
import io.indextables.spark.testutils.TestV1CredentialProvider
import io.indextables.spark.TestBase
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials

/**
 * Unit tests for V1ToV2CredentialsProviderAdapter.
 *
 * These tests verify that the adapter properly:
 *   1. Wraps v1 credential providers 2. Delegates resolveCredentials() to the v1 provider's getCredentials() 3.
 *      Preserves refresh logic by calling getCredentials() on each resolveCredentials() call 4. Handles both session
 *      credentials and basic credentials
 */
class V1ToV2CredentialsProviderAdapterTest extends TestBase {

  private var hadoopConf: Configuration = _
  private var testUri: URI              = _

  override def beforeEach(): Unit = {
    hadoopConf = new Configuration()
    testUri = new URI("s3://test-bucket")
  }

  test("wraps v1 provider and delegates resolveCredentials") {
    hadoopConf.set("test.aws.accessKey", "TEST_ACCESS_KEY")
    hadoopConf.set("test.aws.secretKey", "TEST_SECRET_KEY")
    hadoopConf.set("test.aws.sessionToken", "TEST_SESSION_TOKEN")

    val v1Provider = new TestV1CredentialProvider(testUri, hadoopConf)
    val adapter    = new V1ToV2CredentialsProviderAdapter(v1Provider)

    val creds = adapter.resolveCredentials()

    assert(creds != null)
    assert(creds.isInstanceOf[AwsSessionCredentials])
    val sessionCreds = creds.asInstanceOf[AwsSessionCredentials]
    assert(sessionCreds.accessKeyId() == "TEST_ACCESS_KEY")
    assert(sessionCreds.secretAccessKey() == "TEST_SECRET_KEY")
    assert(sessionCreds.sessionToken() == "TEST_SESSION_TOKEN")
  }

  test("handles basic credentials without session token") {
    hadoopConf.set("test.aws.accessKey", "BASIC_ACCESS_KEY")
    hadoopConf.set("test.aws.secretKey", "BASIC_SECRET_KEY")
    // No session token

    val v1Provider = new TestV1CredentialProvider(testUri, hadoopConf)
    val adapter    = new V1ToV2CredentialsProviderAdapter(v1Provider)

    val creds = adapter.resolveCredentials()

    assert(creds != null)
    assert(!creds.isInstanceOf[AwsSessionCredentials])
    assert(creds.accessKeyId() == "BASIC_ACCESS_KEY")
    assert(creds.secretAccessKey() == "BASIC_SECRET_KEY")
  }

  test("calls v1 provider on each resolveCredentials call (preserves refresh)") {
    // Use a counting provider to verify getCredentials() is called each time
    val callCount = new AtomicInteger(0)

    val countingProvider = new AWSCredentialsProvider {
      override def getCredentials(): AWSCredentials = {
        val count = callCount.incrementAndGet()
        new BasicAWSCredentials(s"ACCESS_KEY_$count", s"SECRET_KEY_$count")
      }
      override def refresh(): Unit = {}
    }

    val adapter = new V1ToV2CredentialsProviderAdapter(countingProvider)

    // First call
    val creds1 = adapter.resolveCredentials()
    assert(creds1.accessKeyId() == "ACCESS_KEY_1")
    assert(callCount.get() == 1)

    // Second call - should call getCredentials() again
    val creds2 = adapter.resolveCredentials()
    assert(creds2.accessKeyId() == "ACCESS_KEY_2")
    assert(callCount.get() == 2)

    // Third call
    val creds3 = adapter.resolveCredentials()
    assert(creds3.accessKeyId() == "ACCESS_KEY_3")
    assert(callCount.get() == 3)
  }

  test("simulates credential refresh scenario") {
    // Simulates a v1 provider that returns different credentials after "refresh"
    var credentialVersion = 1

    val refreshableProvider = new AWSCredentialsProvider {
      override def getCredentials(): AWSCredentials =
        // Simulate: check expiration and refresh if needed (real providers do this internally)
        new BasicSessionCredentials(
          s"ACCESS_V$credentialVersion",
          s"SECRET_V$credentialVersion",
          s"TOKEN_V$credentialVersion"
        )
      override def refresh(): Unit =
        credentialVersion += 1
    }

    val adapter = new V1ToV2CredentialsProviderAdapter(refreshableProvider)

    // Initial credentials
    val creds1 = adapter.resolveCredentials().asInstanceOf[AwsSessionCredentials]
    assert(creds1.accessKeyId() == "ACCESS_V1")
    assert(creds1.sessionToken() == "TOKEN_V1")

    // Simulate refresh (in real scenario, this happens when credentials are near expiration)
    refreshableProvider.refresh()

    // Next call gets refreshed credentials
    val creds2 = adapter.resolveCredentials().asInstanceOf[AwsSessionCredentials]
    assert(creds2.accessKeyId() == "ACCESS_V2")
    assert(creds2.sessionToken() == "TOKEN_V2")
  }

  test("getWrappedProvider returns original v1 provider") {
    hadoopConf.set("test.aws.accessKey", "TEST_ACCESS")
    hadoopConf.set("test.aws.secretKey", "TEST_SECRET")

    val v1Provider = new TestV1CredentialProvider(testUri, hadoopConf)
    val adapter    = new V1ToV2CredentialsProviderAdapter(v1Provider)

    assert(adapter.getWrappedProvider eq v1Provider)
  }

  test("rejects null provider") {
    val exception = intercept[IllegalArgumentException] {
      new V1ToV2CredentialsProviderAdapter(null)
    }
    assert(exception.getMessage.contains("cannot be null"))
  }

  test("rejects non-v1 provider") {
    // Create an object that doesn't implement AWSCredentialsProvider
    val notAProvider = new Object()

    val exception = intercept[IllegalArgumentException] {
      new V1ToV2CredentialsProviderAdapter(notAProvider)
    }
    assert(exception.getMessage.contains("AWSCredentialsProvider"))
  }

  test("multiple adapters can wrap same provider") {
    hadoopConf.set("test.aws.accessKey", "SHARED_ACCESS")
    hadoopConf.set("test.aws.secretKey", "SHARED_SECRET")

    val v1Provider = new TestV1CredentialProvider(testUri, hadoopConf)
    val adapter1   = new V1ToV2CredentialsProviderAdapter(v1Provider)
    val adapter2   = new V1ToV2CredentialsProviderAdapter(v1Provider)

    val creds1 = adapter1.resolveCredentials()
    val creds2 = adapter2.resolveCredentials()

    assert(creds1.accessKeyId() == creds2.accessKeyId())
    assert(creds1.secretAccessKey() == creds2.secretAccessKey())
  }

  test("is thread-safe for concurrent access") {
    val callCount = new AtomicInteger(0)

    val threadSafeProvider = new AWSCredentialsProvider {
      override def getCredentials(): AWSCredentials = {
        callCount.incrementAndGet()
        // Small delay to increase chance of race conditions
        Thread.sleep(1)
        new BasicAWSCredentials("ACCESS", "SECRET")
      }
      override def refresh(): Unit = {}
    }

    val adapter = new V1ToV2CredentialsProviderAdapter(threadSafeProvider)

    // Run 10 concurrent threads, each calling resolveCredentials 10 times
    val threads = (1 to 10).map(_ => new Thread(() => (1 to 10).foreach(_ => adapter.resolveCredentials())))

    threads.foreach(_.start())
    threads.foreach(_.join())

    // All 100 calls should have completed successfully
    assert(callCount.get() == 100)
  }
}
