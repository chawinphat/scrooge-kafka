package main

import scrooge.scrooge_message._
import scrooge.scrooge_networking._

import java.util.Properties
import org.apache.kafka.clients.producer._

object Producer {
  val configReader = new ConfigReader

  def main(args: Array[String]): Unit = {
    while (true) {
      writeToKafka(configReader.getTopic(), configReader.getMessage())
    }
  }

  def writeToKafka(topic: String, message: String): Unit = {
    val message = new CrossChainMessage()
    //  val messageData = new CrossChainMessageData(messageContent)
    //  message.addData(messageData)
    val seralizedMesage = message.toByteArray

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    val producer = new KafkaProducer[String, Array[Byte]](props)
    val record = new ProducerRecord[String, Array[Byte]](topic, seralizedMesage)
    producer.send(record)
    producer.close()
  }
}
