package com.tantivy4spark.debug

// This file analyzes what filter types Spark generates for LIKE patterns
// Based on Spark 3.5.x source code analysis:
//
// SQL LIKE patterns are converted by Spark's optimizer as follows:
// 1. LIKE 'prefix%' -> StringStartsWith filter
// 2. LIKE '%suffix' -> StringEndsWith filter
// 3. LIKE '%contains%' -> StringContains filter
// 4. Complex patterns with _ or mixed % -> No pushdown (stays as Filter in Spark)
//
// The issue is that our current implementation in Tantivy4SparkScanBuilder
// already supports StringStartsWith, StringEndsWith, and StringContains.
// So the problem might be elsewhere - possibly in how we're registering
// our DataSource or how Spark's optimizer sees our capabilities.
//
// However, upon further investigation, it appears that when using SQL,
// Spark might not always convert LIKE to these optimized filters if it
// doesn't know the DataSource supports them. This is different from
// DataFrame API which directly creates StringContains filters.
