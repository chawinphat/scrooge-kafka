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
import java.io.BufferedOutputStream
import java.util.ArrayList
import io.confluent.parallelconsumer.ParallelStreamProcessor.ConsumeProduceResult
import io.confluent.parallelconsumer.ParallelConsumerOptions
import io.confluent.parallelconsumer.ParallelStreamProcessor
import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY
import io.confluent.parallelconsumer.ParallelStreamProcessor.createEosStreamProcessor



object Consumer {
  println("initializing consumer")
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
    println(s"starting kafka consumer node ${nodeId} with broker IPS: ${brokerIps}")
    // if (writeDR) {
    //   println("Disaster Recovery set to True")
    // }

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
  
  def rightPad(input: String, totalLength: Int): String = {
    if (input.length >= totalLength) input
    else input + " " * (totalLength - input.length)
  }


  def consumeFromKafka() = {
    val props = new Properties()
    props.put("bootstrap.servers", brokerIps) // To test locally, change brokerIps with "localhost:9092"
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put("auto.offset.reset", "latest")
    props.put("group.id", System.currentTimeMillis().toString())  // last resort: props.put("group.id", None)

    // var options = ParallelConsumerOptions[String, String].builder().build();
    // props.put("fetch.max.wait.ms", "500")
    // props.put("fetch.min.bytes", "10000000")
    // props.put("max.partition.fetch.bytes", "1000000")
    val consumer = new KafkaConsumer[String, Array[Byte]](props)
    val partitionList = new util.ArrayList[TopicPartition]
    // for(currentIndex <- 0 to rsmSize.ceil.toInt - 1){
    //     println("constructing consumer ...")
    //     println(s"assigning topic ${topic} and partition ${currentIndex}")
    //     partitionList.add(new TopicPartition(topic, currentIndex))
    // }
    // consumer.assign(partitionList)

    val options = ParallelConsumerOptions
        .builder()
        .maxConcurrency(1000)
        .consumer(consumer)
        .build()

    val eosStreamProcessor: ParallelStreamProcessor[String, Array[Byte]] =
      createEosStreamProcessor(options)
    eosStreamProcessor.subscribe(List(topic).asJava)


    val warmupTimer = warmupDuration.seconds.fromNow
    val testTimer = (benchmarkDuration+warmupDuration).seconds.fromNow

    var messagesDeserialized = 0
    var startTime = System.currentTimeMillis()

    var outputContent: Map[String, Double] = Map("Start_Time_MS" -> startTime.toDouble) 
    // println("starting timer")
    var lastPrintMetricTime = System.currentTimeMillis()
    var curPrintMetric = 0
    while (testTimer.hasTimeLeft()) {
      val record = eosStreamProcessor.poll(record => println("Processing a record ..."))
      // for (data <- record.iterator) {
      //   val crossChainMessage = CrossChainMessage.parseFrom(data.value())
      //   val messageDataList = crossChainMessage.data

      //   if (writeDR) {
      //     val transferMessage = ScroogeTransfer().withUnvalidatedCrossChainMessage(crossChainMessage)
      //     val transferBytes = transferMessage.toByteArray
      //     val transferSize = transferBytes.length
      //     val buffer = ByteBuffer.allocate(8)
      //     buffer.order(ByteOrder.LITTLE_ENDIAN)
      //     buffer.putLong(transferSize)

      //     writer.write(buffer.array())
      //     writer.write(transferMessage.toByteArray)
      //   }

      //   if (writeCCF) {
      //     for (msg <- messageDataList) {
      //       val recievedKeyValue = KeyValueHash.parseFrom(msg.messageContent.toByteArray())
      //       val transferMessage = ScroogeTransfer().withKeyValueHash(recievedKeyValue)
      //       val transferBytes = transferMessage.toByteArray
      //       val transferSize = transferBytes.length
      //       val buffer = ByteBuffer.allocate(8)
      //       buffer.order(ByteOrder.LITTLE_ENDIAN)
      //       buffer.putLong(transferSize)

      //       writer.write(buffer.array())
      //       writer.write(transferMessage.toByteArray)
      //     }
      //   }


      //   messageDataList.foreach { messageData =>
      //     val messageContentBytes = messageData.messageContent.toByteArray()
      //     val messageContent = new String(messageContentBytes, "UTF-8")
      //   }

      //   curPrintMetric += data.serializedValueSize()
      //   val curTime = System.currentTimeMillis()

      //   if (curTime - lastPrintMetricTime > 1000) {
      //     println(s"Recv ${curPrintMetric / ((curTime - lastPrintMetricTime)/1000.0) /1024/1024} MBps in last second")
      //     curPrintMetric = 0
      //     lastPrintMetricTime = curTime
      //   }

      //   if (!warmupTimer.hasTimeLeft()) {
      //     messagesDeserialized += 1
      //   }
      // }
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
    outputWriter.writeOutput(jsonString, "/tmp/output.json") // This one is for local read

    // for(i <- 0 to 4){
    //   consumers.get(i).metrics().entrySet().forEach(entry => println(rightPad(s"${entry.getKey().name()}",35) + rightPad(s": ${entry.getValue().metricValue()}",35) + rightPad(s"-- description ${entry.getKey().description()}",30)))
    //   println("")
    // }
  }
  

}
