package main

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

  def writeToKafka(message: String): Unit = {
    val message = new CrossChainMessage()
    val seralizedMesage = message.toByteArray
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
