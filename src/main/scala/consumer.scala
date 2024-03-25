package main

import scrooge.scrooge_message._
import scrooge.scrooge_networking._

import java.util
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties
import scala.collection.JavaConverters._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

object Consumer {

  val configReader = new ConfigReader

  val brokerIps = configReader.getBrokerIps()
  val nodeId = configReader.getNodeId()
  val rsmId = configReader.getRsmId()

  //rsm 1 reads from topic 2, rsm 2 reads from topic 1
  val topic = 
    if (rsmId == 1) {
      configReader.getTopic1()
    } else {
      configReader.getTopic2()
    }

  def main(args: Array[String]): Unit = {
    val timer = 10.seconds.fromNow

    // Run a new thread to measure throughput -> an optimization since polling may take a bit of time
    val throughputMeasurement = Future {
      consumeFromKafka(timer)
    }

    // Wait on new thread to finish
    Await.result(throughputMeasurement, Duration.Inf)
  }

  def consumeFromKafka(timer: Deadline) = {
    val props = new Properties()
    props.put("bootstrap.servers", brokerIps)
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put("auto.offset.reset", "latest")
    props.put("group.id", "consumer-group")
    val consumer: KafkaConsumer[String, Array[Byte]] = new KafkaConsumer[String, Array[Byte]](props)

    var messagesDeserialized = 0
    var startTime = System.currentTimeMillis()

    while (timer.hasTimeLeft()) {
      val record = consumer.poll(1000).asScala
      for (data <- record.iterator) {
        val crossChainMessage = CrossChainMessage.parseFrom(data.value())
        val messageDataList = crossChainMessage.data
        messageDataList.foreach { messageData =>
          val messageContentBytes = messageData.messageContent.toByteArray()
          val messageContent = new String(messageContentBytes, "UTF-8")
          //println("Received Message: " + messageContent)
        }
        messagesDeserialized += 1
      }

      // At every second of the test, meausure how many bytes have been deserialized
      val currentTime = System.currentTimeMillis()
      if (currentTime - startTime >= 1000) {
        val throughput = messagesDeserialized.toDouble / ((currentTime - startTime).toDouble / 1000)
        println(s"Throughput: ${throughput} MPS")
        messagesDeserialized = 0
        startTime = currentTime
      }
    }   
    consumer.close()
  }
}
