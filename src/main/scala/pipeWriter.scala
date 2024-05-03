package main

import java.io.{FileOutputStream, File, PrintWriter}

class PipeWriter {

  def writeOutput(content: String, path: String): Unit = {
    val pipeFile = new File(path)

    val writer = new PrintWriter(new FileOutputStream(pipeFile))
    writer.println(content)
    writer.flush()
    writer.close()
  }
}
