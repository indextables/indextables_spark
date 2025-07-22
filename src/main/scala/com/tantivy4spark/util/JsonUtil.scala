package com.tantivy4spark.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object JsonUtil {
  
  val mapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule)
  
}