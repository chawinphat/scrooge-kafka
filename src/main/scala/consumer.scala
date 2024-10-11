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

  val pipeWriter = new PipeWriter
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
    props.put("bootstrap.servers", brokerIps) // To test locally, change brokerIps with "localhost:9092"
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put("auto.offset.reset", "latest")
    props.put("group.id", nodeId)
    val consumer: KafkaConsumer[String, Array[Byte]] = new KafkaConsumer[String, Array[Byte]](props)
    consumer.subscribe(util.Arrays.asList(topic))

    var messagesDeserialized = 0
    var startTime = System.currentTimeMillis()

    var outputContent: Map[String, Double] = Map("Start_Time_MS" -> startTime.toDouble) 
    println("starting timer")
    while (timer.hasTimeLeft()) {
      val record = consumer.poll(1000).asScala
      println("polling for data")
      for (data <- record.iterator) {
        println("data found")
        val crossChainMessage = CrossChainMessage.parseFrom(data.value())
        val messageDataList = crossChainMessage.data
        messageDataList.foreach { messageData =>
          val messageContentBytes = messageData.messageContent.toByteArray()
          val messageContent = new String(messageContentBytes, "UTF-8")
          println(s"Received Message: $messageContent")
        }
        messagesDeserialized += 1
      }
    }   
    consumer.close()

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
    pipeWriter.writeOutput(jsonString, outputPath) // This one is for Raft
    outputWriter.writeOutput(jsonString, "/tmp/output.json") // This one is for local read
  }
}
