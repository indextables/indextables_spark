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

package io.indextables.spark.sync

import java.io.OutputStream
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets

import com.fasterxml.jackson.databind.ObjectMapper
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}

import org.apache.hadoop.conf.Configuration

import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.rest.CatalogHandlers

/**
 * Lightweight embedded Iceberg REST catalog server for integration tests.
 *
 * <p>Uses {@link HadoopCatalog} backed by the local filesystem and the JDK's built-in
 * {@code com.sun.net.httpserver.HttpServer} (no Docker, no MinIO, no Jetty required). The warehouse
 * is rooted at a local temp directory so all manifest and data files are written to disk and
 * readable by the Rust JNI via the {@code file://} scheme, which is included in
 * {@code iceberg = "0.8"}'s default features.
 *
 * <p>Only read-path REST endpoints are implemented (GET). Writes happen directly against
 * {@link #catalog} via the Iceberg Java API, so the streaming sync's read path sees the latest
 * snapshots without going through HTTP.
 *
 * @param warehouseDir
 *   Absolute local directory to use as the Iceberg warehouse root.
 */
class EmbeddedIcebergRestServer(val warehouseDir: String) extends AutoCloseable {

  private val mapper: ObjectMapper = {
    // RESTObjectMapper is package-private in iceberg-core; access via reflection to get
    // the same fully-configured ObjectMapper that the real Iceberg REST server uses.
    val clazz  = Class.forName("org.apache.iceberg.rest.RESTObjectMapper")
    val method = clazz.getDeclaredMethod("mapper")
    method.setAccessible(true)
    method.invoke(null).asInstanceOf[ObjectMapper]
  }

  /**
   * Backing catalog. Use directly to create tables and commit snapshots in tests.
   *
   * <p>{@link HadoopCatalog} writes every metadata file (version JSON, manifest lists, manifests)
   * to the real filesystem so the Rust JNI can read them directly via {@code file://} URIs.
   */
  val catalog: HadoopCatalog =
    new HadoopCatalog(new Configuration(), s"file://$warehouseDir")

  private val server: HttpServer = {
    val s = HttpServer.create(new InetSocketAddress("localhost", 0), 0)
    s.createContext("/", new RestHandler)
    s.setExecutor(null)
    s.start()
    s
  }

  /** The port this server is listening on (randomly assigned). */
  val port: Int = server.getAddress.getPort

  /** REST URI to supply as {@code spark.indextables.iceberg.uri}. */
  val restUri: String = s"http://localhost:$port"

  override def close(): Unit = {
    server.stop(0)
    catalog.close()
  }

  // ── HTTP handler ────────────────────────────────────────────────────────────

  private class RestHandler extends HttpHandler {

    override def handle(ex: HttpExchange): Unit = {
      try {
        val path   = ex.getRequestURI.getPath
        val method = ex.getRequestMethod
        // Strip /v1/ prefix and split into parts
        val rel   = path.replaceFirst("^/v1/?", "").stripSuffix("/")
        val parts = if (rel.isEmpty) List.empty else rel.split("/", -1).toList

        val (status, body): (Int, String) = (method, parts) match {

          // GET /v1/config
          case ("GET", List("config") | Nil) if rel == "config" || rel.isEmpty =>
            200 -> """{"defaults":{},"overrides":{}}"""

          // GET /v1/namespaces
          case ("GET", List("namespaces")) =>
            val resp = CatalogHandlers.listNamespaces(catalog, Namespace.empty())
            200 -> mapper.writeValueAsString(resp)

          // GET /v1/namespaces/{ns}
          case ("GET", List("namespaces", ns)) =>
            val resp = CatalogHandlers.loadNamespace(catalog, Namespace.of(ns))
            200 -> mapper.writeValueAsString(resp)

          // GET /v1/namespaces/{ns}/tables
          case ("GET", List("namespaces", ns, "tables")) =>
            val resp = CatalogHandlers.listTables(catalog, Namespace.of(ns))
            200 -> mapper.writeValueAsString(resp)

          // GET /v1/namespaces/{ns}/tables/{table}
          case ("GET", List("namespaces", ns, "tables", tbl)) =>
            val tableId = TableIdentifier.of(Namespace.of(ns), tbl)
            val resp    = CatalogHandlers.loadTable(catalog, tableId)
            200 -> mapper.writeValueAsString(resp)

          case ("GET", _) =>
            404 -> s"""{"error":{"message":"Not found: GET /v1/$rel","type":"NoSuchEndpointException","code":404}}"""

          case _ =>
            405 -> """{"error":{"message":"Method not allowed","type":"MethodNotAllowedException","code":405}}"""
        }

        reply(ex, status, body)
      } catch {
        case e: Exception =>
          val msg = s"""{"error":{"message":"${e.getMessage.replace("\"", "'")}","type":"RuntimeException","code":500}}"""
          reply(ex, 500, msg)
      }
    }

    private def reply(ex: HttpExchange, status: Int, json: String): Unit = {
      val bytes = json.getBytes(StandardCharsets.UTF_8)
      ex.getResponseHeaders.set("Content-Type", "application/json")
      ex.sendResponseHeaders(status, bytes.length)
      val out: OutputStream = ex.getResponseBody
      out.write(bytes)
      out.close()
    }
  }
}
