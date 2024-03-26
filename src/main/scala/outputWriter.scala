package main

import upickle.default._
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets

case class OutputFormat(content: String)

/* Macro annotations used by upickle to generate Writer 
 * at compile time. Allows object OutputFormat to be 
 * implicitly availbel to upicke's write function
*/
object OutputFormat {
  implicit val writer: Writer[OutputFormat] = macroW
}

class OutputWriter {

  def writeOutput(content: String, path: String): Unit = {
    val outputData = OutputFormat(content)

    val json = write(outputData)

    val outputPath = Paths.get(path)

    Files.write(outputPath, json.getBytes(StandardCharsets.UTF_8))
  }
}
