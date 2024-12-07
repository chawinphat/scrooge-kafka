package main

import scrooge.scrooge_message._
import scrooge.scrooge_networking._
import scrooge.scrooge_transfer._

import java.util
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties
import scala.collection.JavaConverters._
import java.io.{FileOutputStream, File, PrintWriter}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

import java.nio.{ByteBuffer, ByteOrder}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.MetricName
import java.io.BufferedOutputStream
import org.apache.kafka.common.MetricName

object Consumer {
  // println("initializing consumer")
  val configReader = new ConfigReader
  val rsmId = configReader.getRsmId()
  val nodeId = configReader.getNodeId()
  val topic = if (rsmId == 1) configReader.getTopic2() else configReader.getTopic1()
  val brokerIps = configReader.getBrokerIps()
  val rsmSize = configReader.getRsmSize()
  val benchmarkDuration = configReader.getBenchmarkDuration()
  val warmupDuration = configReader.getWarmupDuration()
  val cooldownDuration = configReader.getCooldownDuration()
  val outputPath = configReader.getOutputPath()
  val writeDR = configReader.getWriteDR()
  val writeCCF = configReader.getWriteCCF()

  val pipeWriter = new PipeWriter
  val outputWriter = new OutputWriter
  val pipeFile = new File(outputPath)
  val writer = new BufferedOutputStream(new FileOutputStream(pipeFile))

  def main(args: Array[String]): Unit = {
    if (topic == "") {
      return;
    }

    println("starting kafka consumer")
    if (writeDR) {
      println("Disaster Recovery set to True")
    }

    // if (writeCCF) {
    //   println("CCF set to true")
    // }

    // Run a new thread to measure throughput -> an optimization since polling may take a bit of time
    val throughputMeasurement = Future {
      consumeFromKafka()
    }
    Await.result(throughputMeasurement, Duration.Inf) // Wait on new thread to finish

    writer.close()
  }

  def consumeFromKafka() = {
    val props = new Properties()
    props.put("bootstrap.servers", brokerIps) // To test locally, change brokerIps with "localhost:9092"
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put("auto.offset.reset", "latest")
    props.put("group.id", nodeId.toString)
    // props.put("fetch.min.bytes", 100000)
    // props.put("fetch.max.wait.ms", 5)
    // props.put("max.poll.records", 10000)

    val consumer: KafkaConsumer[String, Array[Byte]] = new KafkaConsumer[String, Array[Byte]](props)
    println("finished creating consumer")

    val partitionList = new util.ArrayList[TopicPartition]
    for (currentIndex <- 0 to rsmSize.ceil.toInt - 1) {
      print("adding topic (" + topic + ") and partition (" + currentIndex.toString() + ")")
      partitionList.add(new TopicPartition(topic, currentIndex))
    }
    consumer.assign(partitionList)

    val warmupTimer = warmupDuration.seconds.fromNow
    val testTimer = (benchmarkDuration+warmupDuration).seconds.fromNow

    var messagesDeserialized = 0
    var startTime = System.currentTimeMillis()

    var outputContent: Map[String, Double] = Map("Start_Time_MS" -> startTime.toDouble) 
    // println("starting timer")
    while (testTimer.hasTimeLeft()) {
      val record = consumer.poll(1).asScala
      // println("polling for data")
      for (data <- record.iterator) {
        // println("received data from topic: " + data.topic() + ", partition: " + data.partition().toString())
        // println("data found")
        val crossChainMessage = CrossChainMessage.parseFrom(data.value())
        val messageDataList = crossChainMessage.data

        if (writeDR) {
          val transferMessage = ScroogeTransfer().withUnvalidatedCrossChainMessage(crossChainMessage)
          val transferBytes = transferMessage.toByteArray
          val transferSize = transferBytes.length
          val buffer = ByteBuffer.allocate(8)
          buffer.order(ByteOrder.LITTLE_ENDIAN)
          buffer.putLong(transferSize)

          buffer.array().foreach { byte =>
            println(byte & 0xFF) // Convert to unsigned integer representation
          }

          writer.write(buffer.array())
          writer.write(transferMessage.toByteArray)
        }

        if (writeCCF) {
          for (msg <- messageDataList) {
            val recievedKeyValue = KeyValueHash.parseFrom(msg.messageContent.toByteArray())
            val transferMessage = ScroogeTransfer().withKeyValueHash(recievedKeyValue)
            val transferBytes = transferMessage.toByteArray
            val transferSize = transferBytes.length
            val buffer = ByteBuffer.allocate(8)
            buffer.order(ByteOrder.LITTLE_ENDIAN)
            buffer.putLong(transferSize)
            println("creating key")
            println("Key Value" + recievedKeyValue)

            writer.write(buffer.array())
            writer.write(transferMessage.toByteArray)
          }
        }


        messageDataList.foreach { messageData =>
          val messageContentBytes = messageData.messageContent.toByteArray()
          val messageContent = new String(messageContentBytes, "UTF-8")
        }

        if (!warmupTimer.hasTimeLeft()) {
          messagesDeserialized += 1
        }
      }
    }   

    /* Example output:
      { 
        "Final_Time_MS":1714784527496,
        "Total_Messages":0,
        "Overall_Throughput_MPS":0,
        "Start_Time_MS":1714784517478,
        "Final_Elapsed_Time_S":10.018
      }
    */
    val finalTime = System.currentTimeMillis()
    val overallThroughput = messagesDeserialized.toDouble / ((finalTime - startTime).toDouble/1000)

    outputContent += ("Final_Time_MS" -> finalTime.toDouble)
    outputContent += ("Total_Messages" -> messagesDeserialized.toDouble)
    outputContent += ("Final_Elapsed_Time_S" -> ((finalTime - startTime).toDouble/ 1000))   
    outputContent += ("Overall_Throughput_MPS" -> overallThroughput.toDouble)

    val jsonString: String = upickle.default.write(outputContent)
    println(jsonString)

    println("Consumer Metrics: ")
    consumer.metrics().entrySet().forEach(x => println("key=" + x.getKey().name() + ",value=" + x.getValue().metricValue().toString()))
    
    outputWriter.writeOutput(jsonString, "/tmp/output.json") // This one is for local read

    consumer.close()
  }
  

}
