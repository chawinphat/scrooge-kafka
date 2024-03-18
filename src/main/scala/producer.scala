package main

import com.google.protobuf.ByteString

import scrooge.scrooge_message._
import scrooge.scrooge_networking._

import java.util.Properties
import org.apache.kafka.clients.producer._

object Producer {
  val configReader = new ConfigReader

  val brokerIps = configReader.getBrokerIps()
  val nodeId = configReader.getNodeId()
  val rsmId = configReader.getRsmId()
  val rsmSize = configReader.getRsmSize()

  //rsm 1 writes to topic 1 (index 0), rsm 2 writes to topic 2
  val topic = 
    if (rsmId == 1) {
      configReader.getTopic1()
    } else {
      configReader.getTopic2()
    }

  def main(args: Array[String]): Unit = {

    //temporary values until we do linux pipe
    val message = "hello"
    val tempRaftMsgId = 1

    while (true) {
      if (tempRaftMsgId % nodeId == rsmSize) {
        writeToKafka(message)
      }
    }
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
