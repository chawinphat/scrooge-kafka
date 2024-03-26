package main

import com.google.protobuf.ByteString

import scrooge.scrooge_message._
import scrooge.scrooge_networking._

import org.apache.kafka.clients.producer._
import java.util.Properties

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

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

  def main(args: Array[String]): Unit = {

    // if config set to reading from pipe, read from pipe, else set to message
    val message = configReader.getMessage()
    if (configReader.shouldReadFromPipe()) {
      // TODO: implement read from Linux Pipe
    }

    // Warmup period
    val warmup = warmupDuration.seconds.fromNow
    while (warmup.hasTimeLeft()) { } // Do nothing 

    val benchmark = benchmarkDuration.seconds.fromNow
    val produceMessages = Future { // Run on a separate thread
      writeToKafka(message, benchmark)
    }
    Await.result(produceMessages, Duration.Inf) // Wait on new thread to finish

    // Cooldown period
    val cooldown = cooldownDuration.seconds.fromNow
    while (cooldown.hasTimeLeft()) { } // Do nothing
  }

  def writeToKafka(messageString: String, timer: Deadline): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", brokerIps)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    val producer = new KafkaProducer[String, Array[Byte]](props)

    // Delete later when linux pipe implemented
    val tempRaftMsgId = 1

    while (timer.hasTimeLeft()) {
      // For linux piping from raft
      // if (tempRaftMsgId % nodeId == rsmSize) {
      // }

      val messageContent = messageString.getBytes("UTF-8")
      val messageData = CrossChainMessageData (
        messageContent = ByteString.copyFrom(messageContent)
        // Optionally add any other attributes (e.g. sequenceNumber)
      )
      val crossChainMessage = CrossChainMessage (
        data = Seq(messageData)
        // Optionally add any other attributes (e.g. validityProof, ackCount, ackSet)
      )
      val seralizedMesage = crossChainMessage.toByteArray

      val record = new ProducerRecord[String, Array[Byte]](topic, seralizedMesage)
      producer.send(record)
    }

    producer.close()
  }
}
