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
import org.apache.kafka.common.TopicPartition
object Consumer {
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
    props.put("group.id", "consumer-group")
    val consumer: KafkaConsumer[String, Array[Byte]] = new KafkaConsumer[String, Array[Byte]](props)
    /*TopicPartition partition0 = new TopicPartition(topic, 0);
    TopicPartition partition1 = new TopicPartition(topic, 1);
    TopicPartition partition2 = new TopicPartition(topic, 2);
    TopicPartition partition3 = new TopicPartition(topic, 3);
    */
/*    if (nodeId == 0) {
        println(s"Partition 0!")
        consumer.assign(util.Arrays.asList(new TopicPartition(topic, 0)))
    } else if (nodeId == 1) {
        println(s"Partition 1!")
        consumer.assign(util.Arrays.asList(new TopicPartition(topic, 1)))
    } else if (nodeId == 2) {
        println(s"Partition 2!")
        consumer.assign(util.Arrays.asList(new TopicPartition(topic, 2)))
    } else {
        println(s"Partition 3!")
        consumer.assign(util.Arrays.asList(new TopicPartition(topic, 3)))
    }*/
    consumer.subscribe(util.Arrays.asList(topic))

    var messagesDeserialized = 0
    var totalMessages = 0
    var startTime = System.currentTimeMillis()
    var jsonMap: Map[String, Double] = Map("Start_Time_MS" -> startTime.toDouble) 
    var outputContent = ""

    while (timer.hasTimeLeft()) {
      //println(s"Made it inside timer for loop!")
      val record = consumer.poll(1000).asScala
      for (data <- record.iterator) {
        val crossChainMessage = CrossChainMessage.parseFrom(data.value())
        val messageDataList = crossChainMessage.data
        //println(s"Received a message!")
        messageDataList.foreach { messageData =>
          val messageContentBytes = messageData.messageContent.toByteArray()
          val messageContent = new String(messageContentBytes, "UTF-8")
          println(s"Message Contents: $messageContent")
        }
        messagesDeserialized += 1
        totalMessages += 1
      }

      // At every second of the test, meausure how many bytes have been deserialized
      /*val currentTime = System.currentTimeMillis()
      if (currentTime - startTime >= 1000) {
        val throughput = messagesDeserialized.toDouble / ((currentTime - startTime).toDouble / 1000)
        outputContent += s"Throughput: ${throughput} MPS\n"
        //jsonMap += ("Throughput_MPS" -> throughput.toDouble)
        //jsonMap += ("TimeElapsed_MS" -> (currentTime - startTime).toDouble)
        messagesDeserialized = 0
        startTime = currentTime
      }*/
    }
    jsonMap += ("Total_Messages" -> totalMessages.toDouble)
    val finalTime = System.currentTimeMillis()
    val overallThroughput = totalMessages.toDouble / ((finalTime - startTime).toDouble/1000)
    jsonMap += ("Overall_Throughput_MPS" -> overallThroughput.toDouble)
    jsonMap += ("Final_Time_MS" -> finalTime.toDouble)
    jsonMap += ("Final_Elapsed_Time_S" -> ((finalTime - startTime).toDouble/ 1000))   
    consumer.close()

    // println(jsonString)
    // Create json file
    val jsonString: String = upickle.default.write(jsonMap)
    outputWriter.writeOutput(jsonString, outputPath + "output.json")
  }
}
