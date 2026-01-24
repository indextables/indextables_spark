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

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.{JsonNode, MapperFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import org.slf4j.LoggerFactory

/**
 * Utility for schema deduplication in transaction logs and checkpoints.
 *
 * When tables have many splits with the same schema (docMappingJson), storing the full schema in each AddAction wastes
 * significant space. This utility:
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

  /** ObjectMapper for parsing JSON */
  // Configure ObjectMapper to sort properties alphabetically for consistent hashing
  // This ensures that JSON serialization produces the same output regardless of
  // the original field ordering in the input JSON.
  private val mapper: ObjectMapper = {
    val m = new ObjectMapper()
    m.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
    m.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
    m
  }

  // Instrumentation counters for testing/monitoring - track how many times parsing actually happens
  private val parseCounter = new java.util.concurrent.atomic.AtomicLong(0)

  /** Get the number of times filterEmptyObjectMappings has actually parsed JSON (for testing) */
  def getParseCallCount(): Long = parseCounter.get()

  /** Reset the parse call counter (for testing) */
  def resetParseCounter(): Unit = parseCounter.set(0)

  /**
   * Recursively sort all object keys in a JsonNode tree to create a canonical representation.
   *
   * For arrays of objects that have a "name" field (like field definitions in docMappingJson), the array elements are
   * also sorted by their "name" value to ensure consistent ordering regardless of serialization order.
   */
  private def sortJsonNode(node: JsonNode): JsonNode =
    node match {
      case obj: ObjectNode =>
        // Create a new ObjectNode with sorted keys
        val sortedNode = mapper.createObjectNode()
        val fieldNames = obj.fieldNames().asScala.toSeq.sorted
        fieldNames.foreach(fieldName => sortedNode.set[JsonNode](fieldName, sortJsonNode(obj.get(fieldName))))
        sortedNode
      case arr: ArrayNode =>
        // Process array elements recursively first
        val processedElements = arr.elements().asScala.toSeq.map(sortJsonNode)

        // Sort array elements if they are objects with a "name" field
        // This handles field definition arrays like docMappingJson where order is semantically irrelevant
        val sortedElements = if (isNamedObjectArray(processedElements)) {
          processedElements.sortBy(elem => elem.asInstanceOf[ObjectNode].get("name").asText())
        } else {
          // Keep original order for other arrays (primitives, mixed types, objects without "name")
          processedElements
        }

        val sortedArray = mapper.createArrayNode()
        sortedElements.foreach(sortedArray.add)
        sortedArray
      case _ =>
        // Primitives (strings, numbers, booleans, nulls) - return as-is
        node
    }

  /**
   * Check if an array consists of objects that all have a "name" field. This identifies field definition arrays that
   * should be sorted canonically.
   */
  private def isNamedObjectArray(elements: Seq[JsonNode]): Boolean =
    elements.nonEmpty && elements.forall {
      case obj: ObjectNode => obj.has("name")
      case _               => false
    }

  /**
   * Compute a short hash of the schema JSON using canonical normalization.
   *
   * This method normalizes the JSON before hashing to ensure that semantically identical schemas produce the same hash,
   * regardless of property ordering or whitespace differences in the input.
   *
   * The normalization process: 1. Parse the JSON string into a JsonNode tree 2. Recursively sort all object keys
   * alphabetically 3. Serialize back to a canonical form with no extra whitespace 4. Hash the canonical string with
   * SHA-256 5. Base64 encode and truncate to 16 characters (96 bits of entropy)
   *
   * This provides 96 bits of entropy which is sufficient to avoid collisions for any practical number of unique
   * schemas.
   *
   * @param docMappingJson
   *   The schema JSON string
   * @return
   *   A 16-character Base64-encoded hash
   */
  def computeSchemaHash(docMappingJson: String): String = {
    // Normalize the JSON to canonical form for consistent hashing.
    // Uses Jackson with SORT_PROPERTIES_ALPHABETICALLY to ensure object keys
    // are serialized in consistent order. Arrays of named objects are sorted
    // by the "name" field using sortJsonNode().
    val canonicalJson =
      try {
        EnhancedTransactionLogCache.incrementGlobalJsonParseCounter()
        val jsonNode   = mapper.readTree(docMappingJson)
        val sortedNode = sortJsonNode(jsonNode)
        // The mapper is configured with SORT_PROPERTIES_ALPHABETICALLY,
        // so writeValueAsString produces consistent key ordering
        mapper.writeValueAsString(sortedNode)
      } catch {
        case e: Exception =>
          // If JSON parsing fails, fall back to hashing the original string
          logger.warn(s"Failed to parse JSON for canonical hashing, using original: ${e.getMessage}")
          docMappingJson
      }

    val digest    = MessageDigest.getInstance("SHA-256")
    val hashBytes = digest.digest(canonicalJson.getBytes("UTF-8"))
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
    // NOTE: .toList is required to force strict evaluation - Scala 2.12 Seq.map can be lazy
    val deduplicatedActions = actions.map {
      case add: AddAction if add.docMappingJson.isDefined =>
        val schema = add.docMappingJson.get
        val hash   = computeSchemaHash(schema)

        // Add to registry if not already present
        if (!registryBuilder.contains(hash)) {
          registryBuilder(hash) = schema
          logger.debug(s"Schema deduplication: registered schema hash=$hash (${schema.length} bytes)")
        }

        // Replace docMappingJson with docMappingRef
        add.copy(
          docMappingJson = None,
          docMappingRef = Some(hash)
        )

      case other => other
    }.toList // Force STRICT eager evaluation (toList is always strict unlike toSeq in Scala 2.12)

    // Convert registry to configuration format (with prefix)
    val registryMap = registryBuilder.map { case (hash, schema) => (SCHEMA_KEY_PREFIX + hash, schema) }.toMap

    (deduplicatedActions, registryMap)
  }

  /**
   * Filter out object fields with empty field_mappings from a docMappingJson string.
   *
   * When a StructType/ArrayType field is always null during writes, the docMappingJson
   * ends up with an empty field_mappings array for that field. Quickwit's DocMapperBuilder
   * rejects such schemas with "object must have at least one field mapping".
   *
   * This filter removes those problematic fields so aggregation queries can succeed.
   * The removed fields have no indexed data anyway (all values were null).
   *
   * NOTE: This method parses JSON which is expensive. For repeated calls with the same
   * schema, use filterEmptyObjectMappingsCached instead.
   *
   * @param docMappingJson
   *   The original docMappingJson string (array of field mappings)
   * @return
   *   Filtered docMappingJson with empty object fields removed
   */
  def filterEmptyObjectMappings(docMappingJson: String): String =
    try {
      // Track actual parse calls for testing/monitoring
      parseCounter.incrementAndGet()
      EnhancedTransactionLogCache.incrementGlobalJsonParseCounter()
      val rootNode = mapper.readTree(docMappingJson)

      rootNode match {
        case arrayNode: ArrayNode =>
          val filteredArray = mapper.createArrayNode()
          var removedCount  = 0

          arrayNode.elements().asScala.foreach { element =>
            if (isEmptyObjectField(element)) {
              removedCount += 1
              val fieldName = Option(element.get("name")).map(_.asText()).getOrElse("<unknown>")
              logger.debug(s"Filtering out empty object field '$fieldName' from docMappingJson")
            } else {
              filteredArray.add(element)
            }
          }

          if (removedCount > 0) {
            logger.info(s"Filtered $removedCount empty object field(s) from docMappingJson")
          }

          mapper.writeValueAsString(filteredArray)

        case _ =>
          // Not an array, return as-is
          logger.warn(s"docMappingJson is not an array, skipping filter")
          docMappingJson
      }
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to filter docMappingJson, returning original: ${e.getMessage}")
        docMappingJson
    }

  /**
   * Check if a field mapping is an object type with empty field_mappings.
   */
  private def isEmptyObjectField(node: JsonNode): Boolean = {
    val fieldType = Option(node.get("type")).map(_.asText()).getOrElse("")

    if (fieldType == "object") {
      val fieldMappings = node.get("field_mappings")
      fieldMappings match {
        case arr: ArrayNode => arr.size() == 0
        case null           => true // No field_mappings at all
        case _              => false
      }
    } else {
      false
    }
  }

  /**
   * Cached version of filterEmptyObjectMappings that uses the global schema cache.
   *
   * The filterEmptyObjectMappings function parses JSON which is expensive (~1ms per call).
   * Since the same schema hash always produces the same filtered result, we cache based
   * on the schema hash. This provides a massive performance improvement when reading
   * tables with thousands of files that all share the same schema.
   *
   * @param schemaHash
   *   The schema hash (from docMappingRef) - used as cache key
   * @param docMappingJson
   *   The schema JSON to filter
   * @return
   *   Filtered docMappingJson with empty object fields removed
   */
  def filterEmptyObjectMappingsCached(schemaHash: String, docMappingJson: String): String =
    EnhancedTransactionLogCache.getOrComputeFilteredSchema(
      schemaHash,
      filterEmptyObjectMappings(docMappingJson)
    )

  /**
   * Pre-filter an entire schema registry to remove empty object fields.
   *
   * This should be called ONCE when loading a StateManifest, NOT per file entry.
   * The filtered registry can then be passed to toAddActions for simple lookups.
   *
   * Uses cached filtering so each unique schema is only parsed once.
   *
   * @param schemaRegistry
   *   Map of schema hash -> schema JSON
   * @return
   *   Map of schema hash -> filtered schema JSON
   */
  def filterSchemaRegistry(schemaRegistry: Map[String, String]): Map[String, String] = {
    if (schemaRegistry.isEmpty) {
      return schemaRegistry
    }
    logger.debug(s"Pre-filtering schema registry with ${schemaRegistry.size} schemas")
    schemaRegistry.map { case (hash, schema) =>
      hash -> filterEmptyObjectMappingsCached(hash, schema)
    }
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
   * @param filterEmptyObjects
   *   If true, filter out object fields with empty field_mappings (default: true)
   * @return
   *   Actions with docMappingJson restored
   */
  def restoreSchemas(
    actions: Seq[Action],
    registry: Map[String, String],
    filterEmptyObjects: Boolean = true
  ): Seq[Action] = {

    // Build hash -> schema map from registry, applying filter if enabled
    // Use CACHED filtering to avoid repeated JSON parsing for same schema
    val schemaMap = registry.collect {
      case (key, value) if key.startsWith(SCHEMA_KEY_PREFIX) =>
        val hash = key.stripPrefix(SCHEMA_KEY_PREFIX)
        val schema = if (filterEmptyObjects) filterEmptyObjectMappingsCached(hash, value) else value
        hash -> schema
    }

    actions.map {
      case add: AddAction if add.docMappingRef.isDefined && add.docMappingJson.isEmpty =>
        val hash = add.docMappingRef.get
        schemaMap.get(hash) match {
          case Some(schema) =>
            // IMPORTANT: Preserve docMappingRef so DocMappingMetadata cache uses hash as key
            // Clearing it would cause O(n) cache misses (one per file)
            add.copy(docMappingJson = Some(schema))
          case None =>
            logger.warn(s"Schema not found for hash: $hash (path: ${add.path})")
            add // Return unchanged if schema not found
        }

      // Also filter inline docMappingJson (legacy format without deduplication)
      // Use docMappingRef as hash if available to avoid expensive JSON normalization
      case add: AddAction if add.docMappingJson.isDefined && filterEmptyObjects =>
        val schema = add.docMappingJson.get
        val hash = add.docMappingRef.getOrElse(computeSchemaHash(schema))
        val filteredSchema = filterEmptyObjectMappingsCached(hash, schema)
        if (filteredSchema != schema) {
          add.copy(docMappingJson = Some(filteredSchema))
        } else {
          add
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
    val key  = SCHEMA_KEY_PREFIX + hash
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
    val uniqueSize    = uniqueSchemas.map(_.length.toLong).sum
    val refSize       = addActions.count(_.docMappingJson.isDefined) * (HASH_LENGTH + 20L) // hash + JSON overhead

    (originalSize, uniqueSize + refSize)
  }

  /** Threshold for suspecting duplicate schemas from old buggy hash calculation */
  val DUPLICATE_DETECTION_THRESHOLD = 10

  /**
   * Consolidate duplicate schema mappings that may have been created by the old buggy hash calculation.
   *
   * The old hash calculation used raw string hashing, which meant semantically identical JSON schemas with different
   * property ordering would produce different hashes. This method detects and consolidates such duplicates.
   *
   * The consolidation process:
   *   1. Re-hash all schema values using the canonical algorithm 2. Group schemas by their new canonical hash 3. For
   *      duplicate groups, keep one schema and map all old hashes to the new canonical hash 4. Update all AddActions to
   *      reference the new canonical hash 5. Return consolidated registry and updated actions
   *
   * @param actions
   *   Actions to process (AddActions with docMappingRef will be updated)
   * @param registry
   *   Current schema registry from MetadataAction.configuration (with prefix)
   * @return
   *   Tuple of (updated actions, consolidated registry, number of duplicates removed)
   */
  def consolidateDuplicateSchemas(
    actions: Seq[Action],
    registry: Map[String, String]
  ): (Seq[Action], Map[String, String], Int) = {

    // Extract schema entries (hash -> schema)
    val schemaEntries = registry.collect {
      case (key, value) if key.startsWith(SCHEMA_KEY_PREFIX) =>
        key.stripPrefix(SCHEMA_KEY_PREFIX) -> value
    }

    // If below threshold, skip consolidation
    if (schemaEntries.size <= DUPLICATE_DETECTION_THRESHOLD) {
      logger.debug(s"Schema registry has ${schemaEntries.size} entries, below threshold of $DUPLICATE_DETECTION_THRESHOLD - skipping consolidation")
      return (actions, registry, 0)
    }

    logger.info(
      s"Checking ${schemaEntries.size} schema mappings for duplicates (threshold: $DUPLICATE_DETECTION_THRESHOLD)"
    )

    // Re-hash all schemas using canonical algorithm and group by new hash
    // Map: new_canonical_hash -> List[(old_hash, schema)]
    val schemasByCanonicalHash = schemaEntries.toSeq.groupBy { case (_, schema) => computeSchemaHash(schema) }

    // Count how many old hashes map to each new hash
    val duplicateGroups = schemasByCanonicalHash.filter(_._2.size > 1)

    if (duplicateGroups.isEmpty) {
      logger.info("No duplicate schema mappings found - all hashes are canonical")
      return (actions, registry, 0)
    }

    // Build old_hash -> new_canonical_hash mapping
    val hashRemapping       = scala.collection.mutable.Map[String, String]()
    val consolidatedSchemas = scala.collection.mutable.Map[String, String]()

    schemasByCanonicalHash.foreach {
      case (newCanonicalHash, oldHashSchemas) =>
        // Keep the first schema (they're all semantically identical)
        val (_, representativeSchema) = oldHashSchemas.head
        consolidatedSchemas(newCanonicalHash) = representativeSchema

        // Map all old hashes to the new canonical hash
        oldHashSchemas.foreach {
          case (oldHash, _) =>
            if (oldHash != newCanonicalHash) {
              hashRemapping(oldHash) = newCanonicalHash
            }
        }
    }

    val duplicatesRemoved = schemaEntries.size - consolidatedSchemas.size
    logger.info(
      s"Consolidating schema registry: ${schemaEntries.size} entries -> ${consolidatedSchemas.size} entries " +
        s"($duplicatesRemoved duplicates removed, ${duplicateGroups.size} duplicate groups found)"
    )

    // Log details of duplicate groups
    duplicateGroups.foreach {
      case (newHash, oldHashSchemas) =>
        val oldHashes = oldHashSchemas.map(_._1).mkString(", ")
        logger.debug(s"Duplicate group: ${oldHashSchemas.size} old hashes [$oldHashes] -> canonical hash [$newHash]")
    }

    // Update AddActions to use new canonical hashes
    val updatedActions = actions.map {
      case add: AddAction if add.docMappingRef.isDefined =>
        val oldHash = add.docMappingRef.get
        hashRemapping.get(oldHash) match {
          case Some(newHash) =>
            logger.debug(s"Remapping AddAction ${add.path}: $oldHash -> $newHash")
            add.copy(docMappingRef = Some(newHash))
          case None =>
            // Hash is already canonical or not in registry
            add
        }
      case other => other
    }

    // Build new registry with prefix (keep non-schema entries)
    val nonSchemaEntries = registry.filterNot(_._1.startsWith(SCHEMA_KEY_PREFIX))
    val newRegistry = nonSchemaEntries ++ consolidatedSchemas.map {
      case (hash, schema) => (SCHEMA_KEY_PREFIX + hash, schema)
    }

    (updatedActions, newRegistry.toMap, duplicatesRemoved)
  }
}
