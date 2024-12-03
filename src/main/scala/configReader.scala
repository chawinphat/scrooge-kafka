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

  def getTopic1(): String = {
    configData("topic1").str
  }

  def getTopic2(): String = {
    configData("topic2").str
  }

  def getRsmId(): Double = {
    configData("rsm_id").num
  }

  def getNodeId(): Double = {
    configData("node_id").num
  }

  def getBrokerIps(): String = {
    configData("broker_ips").str
  }

  def getRsmSize(): Double = {
    configData("rsm_size").num
  }

  def getMessage(): String = {
    configData("message").str
  }

  def shouldReadFromPipe(): Boolean = {
    configData("read_from_pipe").bool
  }

  def getBenchmarkDuration(): Double = {
    configData("benchmark_duration").num
  }

  def getWarmupDuration(): Double = {
    configData("warmup_duration").num
  }

  def getCooldownDuration(): Double = {
    configData("cooldown_duration").num
  }

  def getInputPath(): String = {
    configData("input_path").str
  }

  def getOutputPath(): String = {
    configData("output_path").str
  }

  def getWriteDR(): Boolean = {
    configData("write_dr").bool
  }

  def getWriteCCF(): Boolean = {
    configData("write_ccf").bool
  }
}

