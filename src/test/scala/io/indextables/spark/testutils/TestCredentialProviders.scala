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

package io.indextables.spark.testutils

import java.net.URI

import org.apache.hadoop.conf.Configuration

// Import real AWS SDK interfaces
import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, BasicAWSCredentials, BasicSessionCredentials}
import software.amazon.awssdk.auth.credentials.{
  AwsBasicCredentials,
  AwsCredentials,
  AwsCredentialsProvider,
  AwsSessionCredentials
}

/**
 * Test implementation of AWS SDK v1 AWSCredentialsProvider. This class uses real AWS SDK v1 interfaces for proper
 * testing.
 */
class TestV1CredentialProvider(uri: URI, conf: Configuration) extends AWSCredentialsProvider {
  private val accessKey    = conf.get("test.aws.accessKey", "TEST_ACCESS_KEY_V1")
  private val secretKey    = conf.get("test.aws.secretKey", "TEST_SECRET_KEY_V1")
  private val sessionToken = Option(conf.get("test.aws.sessionToken"))

  override def getCredentials(): AWSCredentials =
    sessionToken match {
      case Some(token) if token != null && token.trim.nonEmpty =>
        new BasicSessionCredentials(accessKey, secretKey, token)
      case _ =>
        new BasicAWSCredentials(accessKey, secretKey)
    }

  override def refresh(): Unit = {
    // No-op for test implementation
  }
}

/**
 * Test implementation of AWS SDK v2 AwsCredentialsProvider. This class uses real AWS SDK v2 interfaces for proper
 * testing.
 */
class TestV2CredentialProvider(uri: URI, conf: Configuration) extends AwsCredentialsProvider {
  private val accessKey    = conf.get("test.aws.accessKey", "TEST_ACCESS_KEY_V2")
  private val secretKey    = conf.get("test.aws.secretKey", "TEST_SECRET_KEY_V2")
  private val sessionToken = Option(conf.get("test.aws.sessionToken"))

  override def resolveCredentials(): AwsCredentials =
    sessionToken match {
      case Some(token) if token != null && token.trim.nonEmpty =>
        AwsSessionCredentials.create(accessKey, secretKey, token)
      case _ =>
        AwsBasicCredentials.create(accessKey, secretKey)
    }
}

/**
 * Test credential provider that implements both v1 and v2 interfaces. This is useful for testing interface detection
 * logic and ensuring our reflection code properly handles both SDK versions.
 */
class TestDualCredentialProvider(uri: URI, conf: Configuration)
    extends AWSCredentialsProvider
    with AwsCredentialsProvider {

  private val accessKey    = conf.get("test.aws.accessKey", "TEST_DUAL_ACCESS_KEY")
  private val secretKey    = conf.get("test.aws.secretKey", "TEST_DUAL_SECRET_KEY")
  private val sessionToken = Option(conf.get("test.aws.sessionToken"))

  // V1 interface implementation
  override def getCredentials(): AWSCredentials =
    sessionToken match {
      case Some(token) if token != null && token.trim.nonEmpty =>
        new BasicSessionCredentials(accessKey, secretKey, token)
      case _ =>
        new BasicAWSCredentials(accessKey, secretKey)
    }

  override def refresh(): Unit = {
    // No-op for test implementation
  }

  // V2 interface implementation
  override def resolveCredentials(): AwsCredentials =
    sessionToken match {
      case Some(token) if token != null && token.trim.nonEmpty =>
        AwsSessionCredentials.create(accessKey, secretKey, token)
      case _ =>
        AwsBasicCredentials.create(accessKey, secretKey)
    }
}

/** Test provider class that doesn't implement any supported interface */
class UnsupportedProvider(uri: URI, conf: Configuration) {
  // This class intentionally doesn't implement any AWS credential provider interface
}

/** Test provider class with invalid constructor signature */
class InvalidCredentialProvider() {
  // Wrong constructor signature - missing required parameters
}
