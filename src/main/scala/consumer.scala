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
  val rsmId = configReader.getRsmId()
  val nodeId = configReader.getNodeId()
  val topic = if (rsmId == 1) configReader.getTopic1() else configReader.getTopic2()
  val brokerIps = configReader.getBrokerIps()
  val rsmSize = configReader.getRsmSize()
  val benchmarkDuration = configReader.getBenchmarkDuration()
  val warmupDuration = configReader.getWarmupDuration()
  val cooldownDuration = configReader.getCooldownDuration()
  val outputPath = configReader.getOutputPath()

  val outputWriter = new OutputWriter

  def main(args: Array[String]): Unit = {

    // Warmup period
    val warmup = warmupDuration.seconds.fromNow
    while (warmup.hasTimeLeft()) { } // Do nothing

    // Run a new thread to measure throughput -> an optimization since polling may take a bit of time
    val benchmark = benchmarkDuration.seconds.fromNow
    val throughputMeasurement = Future {
      consumeFromKafka(benchmark)
    }
    Await.result(throughputMeasurement, Duration.Inf) // Wait on new thread to finish

    // Cooldown period
    val cooldown = cooldownDuration.seconds.fromNow
    while (cooldown.hasTimeLeft()) { } // Do nothing
  }

  def consumeFromKafka(timer: Deadline) = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put("auto.offset.reset", "latest")
    props.put("group.id", "consumer-group")
    val consumer: KafkaConsumer[String, Array[Byte]] = new KafkaConsumer[String, Array[Byte]](props)
    consumer.subscribe(util.Arrays.asList(topic))

    var messagesDeserialized = 0
    var startTime = System.currentTimeMillis()

    var outputContent = ""

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
        outputContent += s"Throughput: ${throughput} MPS\n"

        messagesDeserialized = 0
        startTime = currentTime
      }
    }   
    consumer.close()

    println(outputContent)
    outputWriter.writeOutput(outputContent, outputPath + "output.json")
  }
}
