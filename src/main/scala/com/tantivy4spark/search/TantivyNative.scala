package com.tantivy4spark.search

object TantivyNative {
  
  @volatile private var loadAttempted = false
  @volatile private var isLoaded = false
  
  def ensureLibraryLoaded(): Boolean = {
    if (!loadAttempted) {
      synchronized {
        if (!loadAttempted) {
          isLoaded = loadLibrary()
          loadAttempted = true
        }
      }
    }
    isLoaded
  }
  
  private def loadLibrary(): Boolean = {
    try {
      // First try loading from java.library.path
      System.loadLibrary("tantivy4spark")
      true
    } catch {
      case _: UnsatisfiedLinkError =>
        try {
          // Try loading from resources
          val resourcePath = "/libtantivy4spark.dylib"
          val inputStream = getClass.getResourceAsStream(resourcePath)
          if (inputStream != null) {
            val tempFile = java.io.File.createTempFile("libtantivy4spark", ".dylib")
            tempFile.deleteOnExit()
            
            val outputStream = new java.io.FileOutputStream(tempFile)
            val buffer = new Array[Byte](1024)
            var bytesRead = inputStream.read(buffer)
            while (bytesRead != -1) {
              outputStream.write(buffer, 0, bytesRead)
              bytesRead = inputStream.read(buffer)
            }
            outputStream.close()
            inputStream.close()
            
            System.load(tempFile.getAbsolutePath)
            true
          } else {
            // Try loading from filesystem relative path
            val libFile = new java.io.File("src/main/resources/libtantivy4spark.dylib")
            if (libFile.exists()) {
              System.load(libFile.getAbsolutePath)
              true
            } else {
              false
            }
          }
        } catch {
          case e: Exception => 
            System.err.println(s"Failed to load Tantivy native library: ${e.getMessage}")
            false
        }
    }
  }
  
  // Initialize library loading on object creation
  ensureLibraryLoaded()

  @native def createIndex(schemaJson: String): Long
  
  @native def createIndexFromComponents(schemaJson: String, componentsJson: String): Long

  @native def addDocument(indexHandle: Long, documentJson: String): Boolean

  @native def commit(indexHandle: Long): Boolean

  @native def search(indexHandle: Long, query: String, limit: Int): String
  
  @native def searchAll(indexHandle: Long, limit: Int): String

  @native def closeIndex(indexHandle: Long): Boolean
  
  @native def getIndexComponents(indexHandle: Long): String
}