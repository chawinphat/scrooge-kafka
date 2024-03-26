package main

import com.google.protobuf.ByteString

import scrooge.scrooge_message._
import scrooge.scrooge_networking._

import org.apache.kafka.clients.producer._
import java.util.Properties

import scala.concurrent.duration._

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

    val tempRaftMsgId = 1

    // Warmup period
    val warmup = warmupDuration.seconds.fromNow
    while (warmup.hasTimeLeft()) { } // Do nothing 

    val benchmark = benchmarkDuration.seconds.fromNow
    while (benchmark.hasTimeLeft()) {
      if (tempRaftMsgId % nodeId == rsmSize) {
        writeToKafka(message)
      }
    }

    // Cooldown period
    val cooldown = cooldownDuration.seconds.fromNow
    while (cooldown.hasTimeLeft()) { } // Do nothing
  }

  def writeToKafka(messageString: String): Unit = {
    val messageContent = messageString.getBytes("UTF-8")

    /* Note: the attributes when creating CrossChainMessageData/CrossChainMessage objects must
      camelCase eventhough it is snake_case in .proto files --> why? because ScalaPB 
      (compiler for the .proto files) converts the field names from snake_case
       to camelCase since Scala uses camelCase */
    val messageData = CrossChainMessageData (
      messageContent = ByteString.copyFrom(messageContent)
      // Optionally add any other attributes (e.g. sequenceNumber)
    )

    val crossChainMessage = CrossChainMessage (
      data = Seq(messageData)
      // Optionally add any other attributes (e.g. validityProof, ackCount, ackSet)
    )


    val seralizedMesage = crossChainMessage.toByteArray
    val props = new Properties()
    props.put("bootstrap.servers", brokerIps)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    val producer = new KafkaProducer[String, Array[Byte]](props)
    val record = new ProducerRecord[String, Array[Byte]](topic, seralizedMesage)
    producer.send(record)
    producer.close()
  }
}
