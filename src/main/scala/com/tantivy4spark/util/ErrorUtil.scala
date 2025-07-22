package com.tantivy4spark.util

import org.slf4j.Logger
import java.io.IOException

object ErrorUtil {
  
  def logAndThrow(logger: Logger, message: String, ex: Throwable): Nothing = {
    logger.error(message, ex)
    throw new IOException(s"$message: ${ex.getMessage}", ex)
  }
  
  def logAndThrowSimple(logger: Logger, message: String, ex: Throwable): Nothing = {
    logger.error(message, ex)
    throw new IOException(message, ex)
  }
  
}