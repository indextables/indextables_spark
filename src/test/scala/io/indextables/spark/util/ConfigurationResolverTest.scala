package io.indextables.spark.util

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ConfigurationResolverTest extends AnyFunSuite with Matchers {

  test("resolveString should return value from first source that has it") {
    val options    = Map("key1" -> "value1").asJava
    val hadoopConf = new Configuration()
    hadoopConf.set("key2", "value2")

    val sources = Seq(
      OptionsConfigSource(options),
      HadoopConfigSource(hadoopConf)
    )

    ConfigurationResolver.resolveString("key1", sources) shouldBe Some("value1")
    ConfigurationResolver.resolveString("key2", sources) shouldBe Some("value2")
  }

  test("resolveString should prioritize earlier sources") {
    val options    = Map("key" -> "value1").asJava
    val hadoopConf = new Configuration()
    hadoopConf.set("key", "value2")

    val sources = Seq(
      OptionsConfigSource(options),
      HadoopConfigSource(hadoopConf)
    )

    ConfigurationResolver.resolveString("key", sources) shouldBe Some("value1")
  }

  test("resolveString should return None if key not found") {
    val options = Map.empty[String, String].asJava
    val sources = Seq(OptionsConfigSource(options))

    ConfigurationResolver.resolveString("missing", sources) shouldBe None
  }

  test("resolveString should skip empty values") {
    val options    = Map("key" -> "").asJava
    val hadoopConf = new Configuration()
    hadoopConf.set("key", "value")

    val sources = Seq(
      OptionsConfigSource(options),
      HadoopConfigSource(hadoopConf)
    )

    ConfigurationResolver.resolveString("key", sources) shouldBe Some("value")
  }

  test("resolveBoolean should parse boolean values") {
    val options = Map(
      "true1"  -> "true",
      "true2"  -> "1",
      "true3"  -> "yes",
      "true4"  -> "on",
      "false1" -> "false",
      "false2" -> "0",
      "false3" -> "no",
      "false4" -> "off"
    ).asJava
    val sources = Seq(OptionsConfigSource(options))

    ConfigurationResolver.resolveBoolean("true1", sources) shouldBe true
    ConfigurationResolver.resolveBoolean("true2", sources) shouldBe true
    ConfigurationResolver.resolveBoolean("true3", sources) shouldBe true
    ConfigurationResolver.resolveBoolean("true4", sources) shouldBe true

    ConfigurationResolver.resolveBoolean("false1", sources) shouldBe false
    ConfigurationResolver.resolveBoolean("false2", sources) shouldBe false
    ConfigurationResolver.resolveBoolean("false3", sources) shouldBe false
    ConfigurationResolver.resolveBoolean("false4", sources) shouldBe false
  }

  test("resolveBoolean should be case insensitive") {
    val options = Map(
      "true1"  -> "TRUE",
      "true2"  -> "True",
      "false1" -> "FALSE",
      "false2" -> "False"
    ).asJava
    val sources = Seq(OptionsConfigSource(options))

    ConfigurationResolver.resolveBoolean("true1", sources) shouldBe true
    ConfigurationResolver.resolveBoolean("true2", sources) shouldBe true
    ConfigurationResolver.resolveBoolean("false1", sources) shouldBe false
    ConfigurationResolver.resolveBoolean("false2", sources) shouldBe false
  }

  test("resolveBoolean should use default for missing keys") {
    val options = Map.empty[String, String].asJava
    val sources = Seq(OptionsConfigSource(options))

    ConfigurationResolver.resolveBoolean("missing", sources, default = true) shouldBe true
    ConfigurationResolver.resolveBoolean("missing", sources, default = false) shouldBe false
  }

  test("resolveBoolean should use default for invalid values") {
    val options = Map("invalid" -> "not-a-boolean").asJava
    val sources = Seq(OptionsConfigSource(options))

    ConfigurationResolver.resolveBoolean("invalid", sources, default = true) shouldBe true
  }

  test("resolveLong should parse long values") {
    val options = Map("num" -> "12345").asJava
    val sources = Seq(OptionsConfigSource(options))

    ConfigurationResolver.resolveLong("num", sources, default = 0L) shouldBe 12345L
  }

  test("resolveLong should use default for invalid values") {
    val options = Map("invalid" -> "not-a-number").asJava
    val sources = Seq(OptionsConfigSource(options))

    ConfigurationResolver.resolveLong("invalid", sources, default = 999L) shouldBe 999L
  }

  test("resolveLong should use default for missing keys") {
    val options = Map.empty[String, String].asJava
    val sources = Seq(OptionsConfigSource(options))

    ConfigurationResolver.resolveLong("missing", sources, default = 123L) shouldBe 123L
  }

  test("resolveInt should parse integer values") {
    val options = Map("num" -> "42").asJava
    val sources = Seq(OptionsConfigSource(options))

    ConfigurationResolver.resolveInt("num", sources, default = 0) shouldBe 42
  }

  test("resolveInt should use default for invalid values") {
    val options = Map("invalid" -> "not-a-number").asJava
    val sources = Seq(OptionsConfigSource(options))

    ConfigurationResolver.resolveInt("invalid", sources, default = 99) shouldBe 99
  }

  test("resolveInt should use default for missing keys") {
    val options = Map.empty[String, String].asJava
    val sources = Seq(OptionsConfigSource(options))

    ConfigurationResolver.resolveInt("missing", sources, default = 10) shouldBe 10
  }

  test("resolveBatch should resolve multiple keys") {
    val options = Map(
      "key1" -> "value1",
      "key2" -> "value2"
    ).asJava
    val sources = Seq(OptionsConfigSource(options))

    val result = ConfigurationResolver.resolveBatch(Seq("key1", "key2", "missing"), sources)

    result shouldBe Map("key1" -> "value1", "key2" -> "value2")
  }

  test("resolveBatch should handle empty results") {
    val options = Map.empty[String, String].asJava
    val sources = Seq(OptionsConfigSource(options))

    val result = ConfigurationResolver.resolveBatch(Seq("key1", "key2"), sources)

    result shouldBe Map.empty
  }

  test("HadoopConfigSource with prefix should prepend prefix to keys") {
    val hadoopConf = new Configuration()
    hadoopConf.set("prefix.key", "value")

    val source = HadoopConfigSource(hadoopConf, "prefix")

    source.get("key") shouldBe Some("value")
  }

  test("HadoopConfigSource without prefix should use key as-is") {
    val hadoopConf = new Configuration()
    hadoopConf.set("key", "value")

    val source = HadoopConfigSource(hadoopConf)

    source.get("key") shouldBe Some("value")
  }

  test("OptionsConfigSource should get values from options map") {
    val options = Map("key" -> "value").asJava
    val source  = OptionsConfigSource(options)

    source.get("key") shouldBe Some("value")
    source.get("missing") shouldBe None
  }

  test("createAWSCredentialSources should create sources in correct order") {
    val options    = Map.empty[String, String].asJava
    val hadoopConf = new Configuration()

    val sources = ConfigurationResolver.createAWSCredentialSources(options, hadoopConf)

    sources should have size 5
    sources(0) shouldBe a[OptionsConfigSource]
    sources(1) shouldBe HadoopConfigSource(hadoopConf, "spark.indextables.aws")
    sources(2) shouldBe HadoopConfigSource(hadoopConf, "spark.hadoop.fs.s3a")
    sources(3) shouldBe HadoopConfigSource(hadoopConf, "fs.s3a")
    sources(4) shouldBe a[EnvironmentConfigSource]
  }

  test("createAzureCredentialSources should create sources in correct order") {
    val options    = Map.empty[String, String].asJava
    val hadoopConf = new Configuration()

    val sources = ConfigurationResolver.createAzureCredentialSources(options, hadoopConf)

    sources should have size 4
    sources(0) shouldBe a[OptionsConfigSource]
    sources(1) shouldBe HadoopConfigSource(hadoopConf, "spark.indextables.azure")
    sources(2) shouldBe HadoopConfigSource(hadoopConf, "")
    sources(3) shouldBe a[EnvironmentConfigSource]
  }

  test("createGeneralConfigSources should create sources in correct order") {
    val options    = Map.empty[String, String].asJava
    val hadoopConf = new Configuration()

    val sources = ConfigurationResolver.createGeneralConfigSources(options, hadoopConf)

    sources should have size 3
    sources(0) shouldBe a[OptionsConfigSource]
    sources(1) shouldBe HadoopConfigSource(hadoopConf, "spark.indextables")
    sources(2) shouldBe HadoopConfigSource(hadoopConf, "")
  }

  // MapConfigSource tests

  test("MapConfigSource should get values from Map") {
    val config = Map("key" -> "value")
    val source = MapConfigSource(config)

    source.get("key") shouldBe Some("value")
    source.get("missing") shouldBe None
  }

  test("MapConfigSource with prefix should prepend prefix to keys") {
    val config = Map("prefix.key" -> "value", "other" -> "other_value")
    val source = MapConfigSource(config, "prefix")

    source.get("key") shouldBe Some("value")
    source.get("other") shouldBe None  // other is not prefixed with "prefix"
  }

  test("MapConfigSource should find key with exact case match") {
    val config = Map("spark.indextables.aws.accessKey" -> "AKIATEST")
    val source = MapConfigSource(config)

    // Should find with exact case match
    source.get("spark.indextables.aws.accessKey") shouldBe Some("AKIATEST")
  }

  test("MapConfigSource should find lowercase key when Map contains lowercase") {
    // When config has lowercase keys (e.g., from CaseInsensitiveStringMap)
    val config = Map("spark.indextables.aws.accesskey" -> "AKIATEST")
    val source = MapConfigSource(config)

    // Should find via lowercase fallback
    source.get("spark.indextables.aws.accessKey") shouldBe Some("AKIATEST")
  }

  test("MapConfigSource should work with ConfigurationResolver") {
    val config = Map(
      "spark.indextables.databricks.workspaceUrl" -> "https://example.databricks.com",
      "spark.indextables.databricks.apiToken" -> "dapi12345"
    )

    val sources = Seq(
      MapConfigSource(config, "spark.indextables.databricks"),
      MapConfigSource(config)
    )

    ConfigurationResolver.resolveString("workspaceUrl", sources) shouldBe Some("https://example.databricks.com")
    ConfigurationResolver.resolveString("apiToken", sources) shouldBe Some("dapi12345")
  }

  test("MapConfigSource should have descriptive name") {
    MapConfigSource(Map.empty).name shouldBe "Map config"
    MapConfigSource(Map.empty, "prefix").name shouldBe "Map config (prefix)"
  }
}
