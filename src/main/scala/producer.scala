package main

import com.google.protobuf.ByteString

import scrooge.scrooge_message._
import scrooge.scrooge_networking._
import scrooge.scrooge_request._

import org.apache.kafka.clients.producer._
import java.util.Properties
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.io.RandomAccessFile

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

import scala.io.Source

object Producer {
  val configReader = new ConfigReader
  val rsmId = configReader.getRsmId()
  val nodeId = configReader.getNodeId()
  val topic = if (rsmId == 1) configReader.getTopic1() else configReader.getTopic2()
  val brokerIps = configReader.getBrokerIps()
  val rsmSize = configReader.getRsmSize()
  val benchmarkDuration = configReader.getBenchmarkDuration()
  val warmupDuration = configReader.getWarmupDuration()
  val cooldownDuration = configReader.getCooldownDuration()
  val inputPath = configReader.getInputPath() // Path to Linux pipe

  def main(args: Array[String]): Unit = {

    // Warmup period
    val warmup = warmupDuration.seconds.fromNow
    while (warmup.hasTimeLeft()) { } // Do nothing 

    val produceMessages = Future { // Run on a separate thread
      writeToKafka()
    }
    Await.result(produceMessages, Duration.Inf) // Wait on new thread to finish

    // Cooldown period
    val cooldown = cooldownDuration.seconds.fromNow
    while (cooldown.hasTimeLeft()) { } // Do nothing
  }

  def writeToKafka(): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", brokerIps)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    val producer = new KafkaProducer[String, Array[Byte]](props)

    if (configReader.shouldReadFromPipe()) { // Send message from Linux pipe
      val linuxPipe = new RandomAccessFile(inputPath, "r")
      val linuxChannel = linuxPipe.getChannel

      val sizeBuffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
      val protobufStrBuffer = ByteBuffer.allocate(4096).order(ByteOrder.LITTLE_ENDIAN)
      val timer = benchmarkDuration.seconds.fromNow
      while (timer.hasTimeLeft()) {
        sizeBuffer.clear()
        protobufStrBuffer.clear()

        while (linuxChannel.read(sizeBuffer) < 8) { }
        sizeBuffer.flip()
        val protobufStrSize = sizeBuffer.getLong

        protobufStrBuffer.limit(protobufStrSize.toInt)
        while (linuxChannel.read(protobufStrBuffer) < protobufStrSize) { }
        protobufStrBuffer.flip()
        val protobufStrBytes = new Array[Byte](protobufStrSize.toInt)
        protobufStrBuffer.get(protobufStrBytes)

        val messageData = CrossChainMessageData.parseFrom(protobufStrBytes)
        if (messageData.sequenceNumber % rsmSize == rsmId) {
          val crossChainMessage = CrossChainMessage (
            data = Seq(messageData)
          )
          val seralizedMesage = crossChainMessage.toByteArray

          val record = new ProducerRecord[String, Array[Byte]](topic, seralizedMesage)
          producer.send(record)
        }
      }

      linuxPipe.close()
    } else { // Send message from config
      val timer = benchmarkDuration.seconds.fromNow

      while (timer.hasTimeLeft()) {
        val messageStr = configReader.getMessage()
  
        val messageStrBytes = messageStr.getBytes("UTF-8")
        val messageData = CrossChainMessageData (
          messageContent = ByteString.copyFrom(messageStrBytes)
          // Optionally add any other attributes (e.g. sequenceNumber)
        )
  
        val crossChainMessage = CrossChainMessage (
          data = Seq(messageData)
        )
        val seralizedMesage = crossChainMessage.toByteArray
  
        val record = new ProducerRecord[String, Array[Byte]](topic, seralizedMesage)
        producer.send(record)
      }
    }
    
    producer.close()
  }
}
