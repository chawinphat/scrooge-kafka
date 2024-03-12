package main

import upickle.default._
import scala.io.Source

case class ConfigData(topic: String, message: String)

class ConfigReader {
  private val jsonString: String = {
    val stream = getClass.getResourceAsStream("/config.json") // Read config.json file located in resources folder
    try {
      Source.fromInputStream(stream).mkString
    } finally {
      stream.close()
    }
  }
  private val configData = ujson.read(jsonString)

  def getTopic(): String = {
    configData("topic").str
  }

  def getMessage(): String = {
    configData("message").str
  }
}