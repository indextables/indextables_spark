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

package io.indextables.spark.transaction

import java.security.MessageDigest
import java.util.Base64

import org.slf4j.LoggerFactory

/**
 * Utility for schema deduplication in transaction logs and checkpoints.
 *
 * When tables have many splits with the same schema (docMappingJson), storing the full schema in each AddAction
 * wastes significant space. This utility:
 *
 *   1. Computes a SHA-256 hash of each schema 2. Replaces docMappingJson with docMappingRef (hash reference) 3. Stores
 *      schemas in MetadataAction.configuration with key "docMappingSchema.<hash>" 4. On read, restores docMappingJson
 *      from the registry
 *
 * This can reduce transaction log size by up to 99% for tables with 400+ column schemas.
 */
object SchemaDeduplication {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Prefix for schema registry keys in MetadataAction.configuration */
  val SCHEMA_KEY_PREFIX = "docMappingSchema."

  /** Hash length (16 characters of Base64 = 96 bits of entropy, virtually collision-free) */
  private val HASH_LENGTH = 16

  /**
   * Compute a short hash of the schema JSON.
   *
   * Uses SHA-256, then Base64 encodes, then truncates to 16 characters. This provides 96 bits of entropy which is
   * sufficient to avoid collisions for any practical number of unique schemas.
   *
   * @param docMappingJson
   *   The schema JSON string
   * @return
   *   A 16-character Base64-encoded hash
   */
  def computeSchemaHash(docMappingJson: String): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val hashBytes = digest.digest(docMappingJson.getBytes("UTF-8"))
    // Use URL-safe Base64 to avoid special characters in keys
    val base64 = Base64.getUrlEncoder.withoutPadding().encodeToString(hashBytes)
    base64.take(HASH_LENGTH)
  }

  /**
   * Deduplicate schemas in a sequence of actions.
   *
   * This replaces docMappingJson with docMappingRef for AddActions, and builds a schema registry map.
   *
   * @param actions
   *   The actions to process
   * @param existingRegistry
   *   Optional existing schema registry (from MetadataAction.configuration)
   * @return
   *   Tuple of (deduplicated actions, schema registry map)
   */
  def deduplicateSchemas(
    actions: Seq[Action],
    existingRegistry: Map[String, String] = Map.empty
  ): (Seq[Action], Map[String, String]) = {

    // Build registry from existing + new schemas
    val registryBuilder = scala.collection.mutable.Map[String, String]()

    // Add existing schemas
    existingRegistry.foreach {
      case (key, value) if key.startsWith(SCHEMA_KEY_PREFIX) =>
        val hash = key.stripPrefix(SCHEMA_KEY_PREFIX)
        registryBuilder(hash) = value
      case _ => // ignore non-schema keys
    }

    // Process actions and collect new schemas
    val deduplicatedActions = actions.map {
      case add: AddAction if add.docMappingJson.isDefined =>
        val schema = add.docMappingJson.get
        val hash = computeSchemaHash(schema)

        // Add to registry if not already present
        if (!registryBuilder.contains(hash)) {
          registryBuilder(hash) = schema
          logger.debug(s"Registered new schema with hash: $hash (${schema.length} bytes)")
        }

        // Replace docMappingJson with docMappingRef
        add.copy(
          docMappingJson = None,
          docMappingRef = Some(hash)
        )

      case other => other
    }

    // Convert registry to configuration format (with prefix)
    val registryMap = registryBuilder.map {
      case (hash, schema) => (SCHEMA_KEY_PREFIX + hash, schema)
    }.toMap

    (deduplicatedActions, registryMap)
  }

  /**
   * Restore schemas in a sequence of actions using a registry.
   *
   * This replaces docMappingRef with docMappingJson for AddActions.
   *
   * @param actions
   *   The actions to process
   * @param registry
   *   The schema registry (from MetadataAction.configuration)
   * @return
   *   Actions with docMappingJson restored
   */
  def restoreSchemas(
    actions: Seq[Action],
    registry: Map[String, String]
  ): Seq[Action] = {

    // Build hash -> schema map from registry
    val schemaMap = registry.collect {
      case (key, value) if key.startsWith(SCHEMA_KEY_PREFIX) =>
        key.stripPrefix(SCHEMA_KEY_PREFIX) -> value
    }

    actions.map {
      case add: AddAction if add.docMappingRef.isDefined && add.docMappingJson.isEmpty =>
        val hash = add.docMappingRef.get
        schemaMap.get(hash) match {
          case Some(schema) =>
            add.copy(
              docMappingJson = Some(schema),
              docMappingRef = None // Clear ref after restoration
            )
          case None =>
            logger.warn(s"Schema not found for hash: $hash (path: ${add.path})")
            add // Return unchanged if schema not found
        }

      case other => other
    }
  }

  /**
   * Check if a schema needs registration in the registry.
   *
   * @param docMappingJson
   *   The schema JSON
   * @param existingRegistry
   *   The current registry (from MetadataAction.configuration)
   * @return
   *   true if the schema is not yet registered
   */
  def needsSchemaRegistration(
    docMappingJson: String,
    existingRegistry: Map[String, String]
  ): Boolean = {
    val hash = computeSchemaHash(docMappingJson)
    val key = SCHEMA_KEY_PREFIX + hash
    !existingRegistry.contains(key)
  }

  /**
   * Extract schema registry from MetadataAction.configuration.
   *
   * @param configuration
   *   The MetadataAction configuration map
   * @return
   *   Map of schema hash -> schema JSON
   */
  def extractSchemaRegistry(configuration: Map[String, String]): Map[String, String] =
    configuration.collect {
      case (key, value) if key.startsWith(SCHEMA_KEY_PREFIX) =>
        key.stripPrefix(SCHEMA_KEY_PREFIX) -> value
    }

  /**
   * Merge new schemas into an existing registry.
   *
   * @param existingRegistry
   *   Current registry (hash -> schema)
   * @param newSchemas
   *   New schemas to add (hash -> schema)
   * @return
   *   Merged registry in configuration format (with prefix)
   */
  def mergeIntoConfiguration(
    existingConfiguration: Map[String, String],
    newSchemas: Map[String, String]
  ): Map[String, String] =
    existingConfiguration ++ newSchemas

  /**
   * Estimate size savings from schema deduplication.
   *
   * @param actions
   *   Actions with docMappingJson
   * @return
   *   Tuple of (original size in bytes, deduplicated size in bytes)
   */
  def estimateSavings(actions: Seq[Action]): (Long, Long) = {
    val addActions = actions.collect { case a: AddAction => a }

    // Original: sum of all docMappingJson sizes
    val originalSize = addActions.flatMap(_.docMappingJson).map(_.length.toLong).sum

    // Deduplicated: unique schemas + refs
    val uniqueSchemas = addActions.flatMap(_.docMappingJson).distinct
    val uniqueSize = uniqueSchemas.map(_.length.toLong).sum
    val refSize = addActions.count(_.docMappingJson.isDefined) * (HASH_LENGTH + 20L) // hash + JSON overhead

    (originalSize, uniqueSize + refSize)
  }
}
