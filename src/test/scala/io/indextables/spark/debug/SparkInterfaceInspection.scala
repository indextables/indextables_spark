package io.indextables.spark.debug

import org.apache.spark.sql.connector.read.SupportsPushDownAggregates

object SparkInterfaceInspection {
  def main(args: Array[String]): Unit = {
    // Get all methods from SupportsPushDownAggregates interface
    val interfaceClass = classOf[SupportsPushDownAggregates]
    val methods        = interfaceClass.getMethods

    println("=== SupportsPushDownAggregates Interface Methods ===")
    methods.foreach { method =>
      println(s"Method: ${method.getName}")
      println(s"  Return type: ${method.getReturnType.getSimpleName}")
      println(s"  Parameters: ${method.getParameterTypes.map(_.getSimpleName).mkString(", ")}")
      println()
    }

    // Also check declared methods (interface-specific)
    val declaredMethods = interfaceClass.getDeclaredMethods
    println("=== Declared Methods ===")
    declaredMethods.foreach { method =>
      println(s"Method: ${method.getName}")
      println(s"  Return type: ${method.getReturnType.getSimpleName}")
      println(s"  Parameters: ${method.getParameterTypes.map(_.getSimpleName).mkString(", ")}")
      println()
    }
  }
}
