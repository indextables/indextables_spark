package io.indextables.spark.util

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ProtocolNormalizerTest extends AnyFunSuite with Matchers {

  test("normalizeS3Protocol should convert s3a:// to s3://") {
    ProtocolNormalizer.normalizeS3Protocol("s3a://bucket/path") shouldBe "s3://bucket/path"
  }

  test("normalizeS3Protocol should convert s3n:// to s3://") {
    ProtocolNormalizer.normalizeS3Protocol("s3n://bucket/path") shouldBe "s3://bucket/path"
  }

  test("normalizeS3Protocol should leave s3:// unchanged") {
    ProtocolNormalizer.normalizeS3Protocol("s3://bucket/path") shouldBe "s3://bucket/path"
  }

  test("normalizeS3Protocol should leave non-S3 paths unchanged") {
    ProtocolNormalizer.normalizeS3Protocol("hdfs://path") shouldBe "hdfs://path"
  }

  test("normalizeAzureProtocol should convert wasb:// to azure://") {
    ProtocolNormalizer.normalizeAzureProtocol(
      "wasb://container@account.blob.core.windows.net/path"
    ) shouldBe "azure://container/path"
  }

  test("normalizeAzureProtocol should convert wasbs:// to azure://") {
    ProtocolNormalizer.normalizeAzureProtocol(
      "wasbs://container@account.blob.core.windows.net/path"
    ) shouldBe "azure://container/path"
  }

  test("normalizeAzureProtocol should convert abfs:// to azure://") {
    ProtocolNormalizer.normalizeAzureProtocol(
      "abfs://container@account.dfs.core.windows.net/path"
    ) shouldBe "azure://container/path"
  }

  test("normalizeAzureProtocol should convert abfss:// to azure://") {
    ProtocolNormalizer.normalizeAzureProtocol(
      "abfss://container@account.dfs.core.windows.net/path"
    ) shouldBe "azure://container/path"
  }

  test("normalizeAzureProtocol should leave azure:// unchanged") {
    ProtocolNormalizer.normalizeAzureProtocol("azure://container/path") shouldBe "azure://container/path"
  }

  test("normalizeAzureProtocol should leave non-Azure paths unchanged") {
    ProtocolNormalizer.normalizeAzureProtocol("s3://bucket/path") shouldBe "s3://bucket/path"
  }

  test("normalizeAllProtocols should normalize S3 paths") {
    ProtocolNormalizer.normalizeAllProtocols("s3a://bucket/path") shouldBe "s3://bucket/path"
  }

  test("normalizeAllProtocols should normalize Azure paths") {
    ProtocolNormalizer.normalizeAllProtocols(
      "abfss://container@account.dfs.core.windows.net/path"
    ) shouldBe "azure://container/path"
  }

  test("normalizeAllProtocols should leave normalized paths unchanged") {
    ProtocolNormalizer.normalizeAllProtocols("s3://bucket/path") shouldBe "s3://bucket/path"
    ProtocolNormalizer.normalizeAllProtocols("azure://container/path") shouldBe "azure://container/path"
  }

  test("isS3Path should detect S3 schemes") {
    ProtocolNormalizer.isS3Path("s3://bucket/path") shouldBe true
    ProtocolNormalizer.isS3Path("s3a://bucket/path") shouldBe true
    ProtocolNormalizer.isS3Path("s3n://bucket/path") shouldBe true
    ProtocolNormalizer.isS3Path("azure://container/path") shouldBe false
  }

  test("isAzurePath should detect Azure schemes") {
    ProtocolNormalizer.isAzurePath("azure://container/path") shouldBe true
    ProtocolNormalizer.isAzurePath("wasb://container@account.blob.core.windows.net/path") shouldBe true
    ProtocolNormalizer.isAzurePath("wasbs://container@account.blob.core.windows.net/path") shouldBe true
    ProtocolNormalizer.isAzurePath("abfs://container@account.dfs.core.windows.net/path") shouldBe true
    ProtocolNormalizer.isAzurePath("abfss://container@account.dfs.core.windows.net/path") shouldBe true
    ProtocolNormalizer.isAzurePath("s3://bucket/path") shouldBe false
  }

  test("normalizeAzureProtocol should handle paths without trailing path") {
    ProtocolNormalizer.normalizeAzureProtocol(
      "wasb://container@account.blob.core.windows.net/"
    ) shouldBe "azure://container/"
    ProtocolNormalizer.normalizeAzureProtocol(
      "abfss://container@account.dfs.core.windows.net/"
    ) shouldBe "azure://container/"
  }

  test("normalizeS3Protocol should handle paths with multiple slashes") {
    ProtocolNormalizer.normalizeS3Protocol("s3a://bucket/path/to/file") shouldBe "s3://bucket/path/to/file"
  }

  test("normalizeAllProtocols should handle empty string") {
    ProtocolNormalizer.normalizeAllProtocols("") shouldBe ""
  }
}
