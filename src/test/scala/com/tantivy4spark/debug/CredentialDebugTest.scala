/*
 * Debug test for credential extraction
 */
package com.tantivy4spark.debug

import com.tantivy4spark.TestBase
import com.tantivy4spark.io.{CloudStorageProviderFactory, CloudStorageConfig}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import java.util.{Collections => JCollections}
import scala.collection.JavaConverters._

class CredentialDebugTest extends TestBase {

  test("debug credential extraction from Spark config") {
    // Set credentials in Spark configuration
    spark.conf.set("spark.indextables.aws.accessKey", "test-access-key")
    spark.conf.set("spark.indextables.aws.secretKey", "test-secret-key")
    spark.conf.set("spark.indextables.s3.endpoint", "http://localhost:9090")
    spark.conf.set("spark.indextables.s3.pathStyleAccess", "true")

    println(s"✅ Set Spark config values")
    println(s"   - accessKey: ${spark.conf.get("spark.indextables.aws.accessKey")}")
    println(s"   - endpoint: ${spark.conf.get("spark.indextables.s3.endpoint")}")

    // Try different ways to read the pathStyleAccess config
    try {
      val pathStyleDirect = spark.conf.get("spark.indextables.s3.pathStyleAccess")
      println(s"   - pathStyleAccess (direct): $pathStyleDirect")
    } catch {
      case ex: Exception =>
        println(s"   - pathStyleAccess (direct): FAILED - ${ex.getMessage}")
    }

    try {
      val pathStyleWithDefault = spark.conf.get("spark.indextables.s3.pathStyleAccess", "default-false")
      println(s"   - pathStyleAccess (with default): $pathStyleWithDefault")
    } catch {
      case ex: Exception =>
        println(s"   - pathStyleAccess (with default): FAILED - ${ex.getMessage}")
    }

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    println(s"✅ Hadoop config before:")
    println(s"   - spark.indextables.aws.accessKey: ${hadoopConf.get("spark.indextables.aws.accessKey")}")
    println(s"   - spark.hadoop.fs.s3a.access.key: ${hadoopConf.get("spark.hadoop.fs.s3a.access.key")}")

    // Test direct config creation
    val options = new CaseInsensitiveStringMap(JCollections.emptyMap())

    try {
      val provider = CloudStorageProviderFactory.createProvider(
        "s3://test-bucket/test-path",
        options,
        hadoopConf
      )

      println(s"✅ Successfully created cloud storage provider: ${provider.getProviderType}")

      // Test basic file operations to see if credentials work
      try {
        provider.exists("s3://test-bucket/nonexistent-file")
        println("✅ Credentials seem to be working (exists check succeeded)")
      } catch {
        case ex: Exception =>
          println(s"❌ Credentials issue detected: ${ex.getMessage}")
      }

      provider.close()
    } catch {
      case ex: Exception =>
        println(s"❌ Failed to create provider: ${ex.getMessage}")
        ex.printStackTrace()
    }
  }
}
